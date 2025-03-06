package telegramfile

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"io"
	"math/rand"
	"os"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"time"

	"go.uber.org/zap"

	"crypto/sha256"
	"encoding/hex"

	"github.com/gotd/contrib/bg"
	"github.com/gotd/td/telegram"
	"github.com/gotd/td/tg"
)

// Config holds the configuration for Telegram API access
type Config struct {
	AppID     int
	AppHash   string
	BotTokens []string
}

// UploadResult contains information about the uploaded file
type UploadResult struct {
	MessageID int
	FileName  string
	FileSize  int64
	SHA256    string
}

// MessageInfo contains basic information about a message
type MessageInfo struct {
	ID        int
	Date      time.Time
	HasMedia  bool
	MediaType string
	Text      string
	FileSize  int64
	FileName  string
}

// AuthenticatedClient creates and authenticates a Telegram client with the given configuration
func AuthenticatedClient(ctx context.Context, config Config) (*tg.Client, func() error, error) {
	logger, err := zap.NewDevelopment()
	if err != nil {
		return nil, nil, fmt.Errorf("failed to create logger: %w", err)
	}

	var client *telegram.Client
	var tgClient *tg.Client

	for i, botToken := range config.BotTokens {
		select {
		case <-ctx.Done():
			return nil, nil, fmt.Errorf("context canceled before connection: %w", ctx.Err())
		default:
		}

		client = telegram.NewClient(config.AppID, config.AppHash, telegram.Options{})
		stop, err := bg.Connect(client)
		if err != nil {
			logger.Warn("Failed to connect with bot", zap.Int("bot_index", i), zap.Error(err))
			continue
		}

		err = authenticateWithRetry(ctx, client, botToken, logger)
		if err != nil {
			logger.Warn("Failed to authenticate with bot", zap.Int("bot_index", i), zap.Error(err))
			_ = stop()
			continue
		}

		tgClient = client.API()
		logger.Info("Successfully authenticated with bot", zap.Int("bot_index", i+1))
		return tgClient, stop, nil
	}

	return nil, nil, fmt.Errorf("all bots failed to authenticate")
}

// GetFileReaderFromMessage returns an io.ReadCloser for a file attached to a message in a Telegram channel
func GetFileReaderFromMessage(ctx context.Context, client *tg.Client, channelID int64, messageID int) (io.ReadCloser, error) {
	maxRetries := 3
	var lastErr error

	for retry := 0; retry < maxRetries; retry++ {
		channel, err := getChannelByID(ctx, client, channelID)
		if err != nil {
			lastErr = fmt.Errorf("failed to get channel: %w", err)
			continue
		}

		message, err := getMessageByID(ctx, client, channelID, channel.AccessHash, messageID)
		if err != nil {
			lastErr = fmt.Errorf("failed to get message: %w", err)
			continue
		}

		location, fileSize, err := getLocation(ctx, client, channelID, message.ID)
		if err != nil {
			lastErr = fmt.Errorf("failed to get file location: %w", err)
			continue
		}

		reader, err := getFileReader(ctx, client, location, fileSize)
		if err != nil {
			lastErr = fmt.Errorf("failed to create file reader: %w", err)
			continue
		}

		return &readCloserWithNoopClose{
			Reader: reader,
		}, nil
	}

	return nil, fmt.Errorf("failed after %d retries: %w", maxRetries, lastErr)
}

// DownloadFile downloads a file from Telegram to a local path with progress reporting
func DownloadFile(ctx context.Context, client *tg.Client, channelID int64, messageID int, outputPath string) error {
	reader, err := GetFileReaderFromMessage(ctx, client, channelID, messageID)
	if err != nil {
		return err
	}
	defer reader.Close()

	file, err := os.Create(outputPath)
	if err != nil {
		return fmt.Errorf("failed to create output file: %w", err)
	}
	defer file.Close()

	// Add progress reporting
	progressChan := make(chan int64)
	progressCtx, progressCancel := context.WithCancel(ctx)
	defer progressCancel()

	go func() {
		var totalProgress int64
		var lastReportedProgress int64
		ticker := time.NewTicker(5 * time.Second)
		defer ticker.Stop()

		startTime := time.Now()

		for {
			select {
			case progress := <-progressChan:
				totalProgress = progress
			case <-ticker.C:
				now := time.Now()
				progressDelta := totalProgress - lastReportedProgress
				if progressDelta > 0 {
					intervalSpeed := float64(progressDelta) / 5.0 / 1024 / 1024
					averageSpeed := float64(totalProgress) / now.Sub(startTime).Seconds() / 1024 / 1024

					fmt.Printf("Downloaded: %d bytes (Current: %.2f MB/s, Average: %.2f MB/s)\n",
						totalProgress, intervalSpeed, averageSpeed)

					lastReportedProgress = totalProgress
				}
			case <-progressCtx.Done():
				return
			}
		}
	}()

	bufferedWriter := bufio.NewWriter(file)

	type progressWriter struct {
		progressChan chan<- int64
		total        int64
	}

	write := func(pw *progressWriter, p []byte) (int, error) {
		n := len(p)
		pw.total += int64(n)
		pw.progressChan <- pw.total
		return n, nil
	}

	pw := &progressWriter{progressChan: progressChan}

	teeReader := io.TeeReader(reader, &writerFunc{
		writeFunc: func(p []byte) (int, error) {
			return write(pw, p)
		},
	})

	buffer := make([]byte, 1024*1024)
	written, err := io.CopyBuffer(bufferedWriter, teeReader, buffer)

	if err != nil {
		if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
			return fmt.Errorf("download interrupted: %w", err)
		}
		return fmt.Errorf("failed to copy file: %w", err)
	}

	if err := bufferedWriter.Flush(); err != nil {
		return fmt.Errorf("failed to flush buffer: %w", err)
	}

	fmt.Printf("Successfully downloaded %d bytes\n", written)
	return nil
}

// UploadFile uploads a file to a Telegram channel and returns the message ID
func UploadFile(ctx context.Context, client *tg.Client, channelID int64, filePath string) (*UploadResult, error) {
	channel, err := getChannelByID(ctx, client, channelID)
	if err != nil {
		return nil, fmt.Errorf("failed to get channel: %w", err)
	}

	sha256Hash, err := calculateFileSHA256(filePath)
	if err != nil {
		return nil, fmt.Errorf("failed to calculate SHA256 hash: %w", err)
	}

	file, err := os.Open(filePath)
	if err != nil {
		return nil, fmt.Errorf("failed to open file: %w", err)
	}
	defer file.Close()

	fileInfo, err := file.Stat()
	if err != nil {
		return nil, fmt.Errorf("failed to get file info: %w", err)
	}

	if fileInfo.Size() > 2000*1024*1024 {
		return nil, fmt.Errorf("file is too large: %.2f MB (max 2000 MB)", float64(fileInfo.Size())/1024/1024)
	}

	buffer := make([]byte, 512*1024)
	var totalBytes int64
	partNum := 0
	fileID := rand.Int63()

	for {
		n, err := file.Read(buffer)
		if err == io.EOF {
			break
		}
		if err != nil {
			return nil, fmt.Errorf("failed to read file: %w", err)
		}

		chunk := make([]byte, n)
		copy(chunk, buffer[:n])

		_, err = client.UploadSaveFilePart(ctx, &tg.UploadSaveFilePartRequest{
			FileID:   fileID,
			FilePart: partNum,
			Bytes:    chunk,
		})
		if err != nil {
			return nil, fmt.Errorf("failed to upload file part %d: %w", partNum, err)
		}

		totalBytes += int64(n)
		partNum++

		fmt.Printf("Uploading... %.2f%% (%.2f MB)\n",
			float64(totalBytes)*100/float64(fileInfo.Size()),
			float64(totalBytes)/1024/1024)
	}

	inputFile := &tg.InputFile{
		ID:          fileID,
		Parts:       partNum,
		Name:        fileInfo.Name(),
		MD5Checksum: "",
	}

	inputMedia := &tg.InputMediaUploadedDocument{
		File: inputFile,
		Attributes: []tg.DocumentAttributeClass{
			&tg.DocumentAttributeFilename{
				FileName: fileInfo.Name(),
			},
		},
	}

	message, err := client.MessagesSendMedia(ctx, &tg.MessagesSendMediaRequest{
		Silent:     false,
		Background: false,
		ClearDraft: false,
		Peer: &tg.InputPeerChannel{
			ChannelID:  channelID,
			AccessHash: channel.AccessHash,
		},
		RandomID: rand.Int63(),
		Media:    inputMedia,
		Message:  fmt.Sprintf("Uploaded file: %s", fileInfo.Name()),
	})
	if err != nil {
		return nil, fmt.Errorf("failed to send media: %w", err)
	}

	var messageID int
	switch m := message.(type) {
	case *tg.Updates:
		for _, update := range m.Updates {
			if msg, ok := update.(*tg.UpdateNewChannelMessage); ok {
				if message, ok := msg.Message.(*tg.Message); ok {
					messageID = message.ID
					break
				}
			}
		}
	}

	if messageID == 0 {
		return nil, fmt.Errorf("failed to get message ID from response")
	}

	return &UploadResult{
		MessageID: messageID,
		FileName:  fileInfo.Name(),
		FileSize:  fileInfo.Size(),
		SHA256:    sha256Hash,
	}, nil
}

// GetLastMessages retrieves the last 'limit' messages from a Telegram channel
func GetLastMessages(ctx context.Context, client *tg.Client, channelID int64, limit int) ([]MessageInfo, error) {
	channel, err := getChannelByID(ctx, client, channelID)
	if err != nil {
		return nil, fmt.Errorf("failed to get channel: %w", err)
	}

	ranges := []struct {
		start int
		end   int
	}{
		{1, limit},
		{35, 35 + limit},
		{100, 100 + limit},
		{1000, 1000 + limit},
		{10000, 10000 + limit},
		{100000, 100000 + limit},
	}

	var foundMessages []MessageInfo

	for _, r := range ranges {
		var messageIDs []int
		for id := r.start; id < r.end; id++ {
			messageIDs = append(messageIDs, id)
		}

		var inputMsgIDs []tg.InputMessageClass
		for _, id := range messageIDs {
			inputMsgIDs = append(inputMsgIDs, &tg.InputMessageID{ID: id})
		}

		req := &tg.ChannelsGetMessagesRequest{
			Channel: channel,
			ID:      inputMsgIDs,
		}

		resp, err := client.ChannelsGetMessages(ctx, req)
		if err != nil {
			fmt.Printf("Failed to get messages in range: %v\n", err)
			continue
		}

		var msgList []tg.MessageClass
		switch res := resp.(type) {
		case *tg.MessagesChannelMessages:
			msgList = res.Messages
		case *tg.MessagesMessages:
			msgList = res.Messages
		default:
			fmt.Printf("Unexpected response type: %T\n", resp)
			continue
		}

		for _, msg := range msgList {
			message, ok := msg.(*tg.Message)
			if !ok {
				continue
			}

			if message.ID == 0 {
				continue
			}

			info := MessageInfo{
				ID:   message.ID,
				Date: time.Unix(int64(message.Date), 0),
				Text: message.Message,
			}

			if message.Media != nil {
				info.HasMedia = true

				switch media := message.Media.(type) {
				case *tg.MessageMediaDocument:
					info.MediaType = "Document"
					if doc, ok := media.Document.(*tg.Document); ok {
						info.FileSize = doc.Size

						for _, attr := range doc.Attributes {
							if fileAttr, ok := attr.(*tg.DocumentAttributeFilename); ok {
								info.FileName = fileAttr.FileName
								break
							}
						}
					}
				case *tg.MessageMediaPhoto:
					info.MediaType = "Photo"
				case *tg.MessageMediaGeo:
					info.MediaType = "Location"
				case *tg.MessageMediaContact:
					info.MediaType = "Contact"
				case *tg.MessageMediaPoll:
					info.MediaType = "Poll"
				case *tg.MessageMediaWebPage:
					info.MediaType = "WebPage"
				default:
					info.MediaType = fmt.Sprintf("%T", media)
				}
			}

			foundMessages = append(foundMessages, info)
		}

		if len(foundMessages) > 0 {
			fmt.Printf("Found messages in range: %d to %d, count: %d\n", r.start, r.end, len(foundMessages))
			break
		}
	}

	sort.Slice(foundMessages, func(i, j int) bool {
		return foundMessages[i].ID > foundMessages[j].ID
	})

	if len(foundMessages) > limit {
		foundMessages = foundMessages[:limit]
	}

	return foundMessages, nil
}

// Internal types and functions below

type readCloserWithCleanup struct {
	io.Reader
	cleanup func()
}

func (r *readCloserWithCleanup) Close() error {
	r.cleanup()
	return nil
}

type writerFunc struct {
	writeFunc func([]byte) (int, error)
}

func (w *writerFunc) Write(p []byte) (int, error) {
	return w.writeFunc(p)
}

type chunkReader struct {
	ctx       context.Context
	client    *tg.Client
	location  tg.InputFileLocationClass
	offset    int64
	chunk     []byte
	chunkPos  int
	chunkSize int64
	fileSize  int64
}

func (r *chunkReader) Read(p []byte) (n int, err error) {
	if r.offset >= r.fileSize {
		return 0, io.EOF
	}

	if r.chunk == nil || r.chunkPos >= len(r.chunk) {
		select {
		case <-r.ctx.Done():
			return 0, fmt.Errorf("context canceled while downloading chunk: %w", r.ctx.Err())
		default:
		}

		chunkSize := r.chunkSize

		chunk, err := getChunk(r.ctx, r.client, r.location, r.offset, chunkSize)
		if err != nil {
			return 0, err
		}
		if len(chunk) == 0 {
			return 0, io.EOF
		}
		r.chunk = chunk
		r.chunkPos = 0
		r.offset += int64(len(chunk))
	}

	n = copy(p, r.chunk[r.chunkPos:])
	r.chunkPos += n
	return n, nil
}

func calculateFileSHA256(filePath string) (string, error) {
	file, err := os.Open(filePath)
	if err != nil {
		return "", fmt.Errorf("failed to open file: %w", err)
	}
	defer file.Close()

	hash := sha256.New()

	if _, err := io.Copy(hash, file); err != nil {
		return "", fmt.Errorf("failed to calculate hash: %w", err)
	}

	hashSum := hash.Sum(nil)
	hashString := hex.EncodeToString(hashSum)

	return hashString, nil
}

func authenticateWithRetry(ctx context.Context, client *telegram.Client, botToken string, log *zap.Logger) error {
	maxWaitSeconds := 30

	_, err := client.Auth().Bot(ctx, botToken)
	if err == nil {
		return nil
	}

	if strings.Contains(err.Error(), "FLOOD_WAIT") {
		waitTime := time.Second
		if matches := regexp.MustCompile(`FLOOD_WAIT \((\d+)\)`).FindStringSubmatch(err.Error()); len(matches) > 1 {
			if seconds, parseErr := strconv.Atoi(matches[1]); parseErr == nil {
				waitTime = time.Duration(seconds) * time.Second
			}
		}

		if waitTime.Seconds() > float64(maxWaitSeconds) {
			log.Info("Required wait time too long, rotating to next bot",
				zap.Float64("wait_seconds", waitTime.Seconds()),
				zap.Int("max_wait_seconds", maxWaitSeconds))
			return fmt.Errorf("rate limit requires waiting %.1f seconds, rotating to next bot", waitTime.Seconds())
		}

		waitTime += time.Second
		log.Info("Rate limited during authentication, waiting exact time plus 1 second safety margin",
			zap.Duration("wait_time", waitTime),
			zap.String("wait_duration_human", waitTime.String()))

		select {
		case <-time.After(waitTime):
			_, err := client.Auth().Bot(ctx, botToken)
			return err
		case <-ctx.Done():
			return ctx.Err()
		}
	}

	return err
}

func getChannelByID(ctx context.Context, client *tg.Client, channelID int64) (*tg.InputChannel, error) {
	inputChannel := &tg.InputChannel{
		ChannelID: channelID,
	}
	channels, err := client.ChannelsGetChannels(ctx, []tg.InputChannelClass{inputChannel})
	if err != nil {
		return nil, err
	}

	if len(channels.GetChats()) == 0 {
		return nil, fmt.Errorf("channel not found")
	}
	return channels.GetChats()[0].(*tg.Channel).AsInput(), nil
}

func getMessageByID(ctx context.Context, client *tg.Client, channelID, accessHash int64, messageID int) (*tg.Message, error) {
	input := &tg.InputChannel{
		ChannelID:  channelID,
		AccessHash: accessHash,
	}

	messages, err := client.ChannelsGetMessages(ctx, &tg.ChannelsGetMessagesRequest{
		Channel: input,
		ID: []tg.InputMessageClass{
			&tg.InputMessageID{
				ID: messageID,
			},
		},
	})
	if err != nil {
		return nil, fmt.Errorf("failed to get message: %w", err)
	}

	channelMessages, ok := messages.(*tg.MessagesChannelMessages)
	if !ok {
		return nil, fmt.Errorf("unexpected type: %T", messages)
	}

	if len(channelMessages.Messages) == 0 {
		return nil, fmt.Errorf("message not found")
	}

	message, ok := channelMessages.Messages[0].(*tg.Message)
	if !ok {
		return nil, fmt.Errorf("not a message: %T", channelMessages.Messages[0])
	}

	return message, nil
}

func getLocation(ctx context.Context, client *tg.Client, channelID int64, messageID int) (location *tg.InputDocumentFileLocation, size int64, err error) {
	channel, err := getChannelByID(ctx, client, channelID)
	if err != nil {
		return nil, 0, err
	}

	messageRequest := tg.ChannelsGetMessagesRequest{
		Channel: channel,
		ID:      []tg.InputMessageClass{&tg.InputMessageID{ID: messageID}},
	}

	res, err := client.ChannelsGetMessages(ctx, &messageRequest)
	if err != nil {
		return nil, 0, err
	}

	messages, _ := res.(*tg.MessagesChannelMessages)
	if len(messages.Messages) == 0 {
		return nil, 0, errors.New("no messages found")
	}

	switch item := messages.Messages[0].(type) {
	case *tg.MessageEmpty:
		return nil, 0, errors.New("no messages found")
	case *tg.Message:
		media, ok := item.Media.(*tg.MessageMediaDocument)
		if !ok {
			return nil, 0, errors.New("message does not contain a document")
		}
		document, ok := media.Document.(*tg.Document)
		if !ok {
			return nil, 0, errors.New("media is not a document")
		}
		location = document.AsInputDocumentFileLocation()
		return location, document.Size, nil
	}

	return nil, 0, errors.New("unexpected message type")
}

func getChunk(ctx context.Context, client *tg.Client, location tg.InputFileLocationClass, offset int64, limit int64) ([]byte, error) {
	maxRetries := 5

	for retry := 0; retry < maxRetries; retry++ {
		select {
		case <-ctx.Done():
			return nil, fmt.Errorf("context canceled while getting chunk: %w", ctx.Err())
		default:
		}

		req := &tg.UploadGetFileRequest{
			Offset:   offset,
			Limit:    int(limit),
			Location: location,
			Precise:  true,
		}

		r, err := client.UploadGetFile(ctx, req)
		if err != nil {
			if strings.Contains(err.Error(), "connection dead") {
				fmt.Printf("Connection error, retrying in 1 second (attempt %d/%d)\n", retry+1, maxRetries)
				time.Sleep(time.Second)
				continue
			}

			if strings.Contains(err.Error(), "FLOOD_WAIT") {
				waitTime := time.Second
				telegramWaitSeconds := 0
				if matches := regexp.MustCompile(`FLOOD_WAIT \((\d+)\)`).FindStringSubmatch(err.Error()); len(matches) > 1 {
					if seconds, parseErr := strconv.Atoi(matches[1]); parseErr == nil {
						telegramWaitSeconds = seconds
						waitTime = time.Duration(seconds) * time.Second
					}
				}

				waitTime += time.Second
				fmt.Printf("Rate limited, Telegram expects us to wait %d seconds, waiting %v before retry %d/%d\n",
					telegramWaitSeconds, waitTime, retry+1, maxRetries)

				timer := time.NewTimer(waitTime)
				select {
				case <-timer.C:
				case <-ctx.Done():
					timer.Stop()
					return nil, fmt.Errorf("context canceled while waiting for rate limit: %w", ctx.Err())
				}
				continue
			}
			return nil, err
		}

		switch result := r.(type) {
		case *tg.UploadFile:
			return result.Bytes, nil
		default:
			return nil, fmt.Errorf("unexpected type %T", r)
		}
	}

	return nil, fmt.Errorf("max retries exceeded while getting chunk at offset %d", offset)
}

func newChunkReader(ctx context.Context, client *tg.Client, location tg.InputFileLocationClass, fileSize int64) *chunkReader {
	return &chunkReader{
		ctx:       ctx,
		client:    client,
		location:  location,
		chunkSize: 512 * 1024,
		fileSize:  fileSize,
	}
}

func getFileReader(ctx context.Context, client *tg.Client, location tg.InputFileLocationClass, fileSize int64) (io.Reader, error) {
	return newChunkReader(ctx, client, location, fileSize), nil
}

func withClient(ctx context.Context, config Config, timeout time.Duration) (context.Context, *tg.Client, func(), error) {
	tgClient, stop, err := AuthenticatedClient(ctx, config)
	if err != nil {
		return nil, nil, nil, err
	}

	deadline, hasDeadline := ctx.Deadline()
	if !hasDeadline || time.Until(deadline) > timeout {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, timeout)
		return ctx, tgClient, func() {
			cancel()
			_ = stop()
		}, nil
	}

	cleanup := func() {
		_ = stop()
	}

	return ctx, tgClient, cleanup, nil
}

// readCloserWithNoopClose wraps an io.Reader to implement io.ReadCloser with a no-op Close method
type readCloserWithNoopClose struct {
	io.Reader
}

func (r *readCloserWithNoopClose) Close() error {
	return nil
}
