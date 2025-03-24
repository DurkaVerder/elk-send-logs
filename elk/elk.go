package elk

import (
	"context"
	"fmt"
	"log"
	"net"
	"sync"
	"time"

	logrustash "github.com/bshuster-repo/logrus-logstash-hook"
	"github.com/sirupsen/logrus"
)

var Log *Logger

type Logger struct {
	mu      *sync.Mutex
	logger  *logrus.Logger
	logChan chan LogMessage
	conn    net.Conn
	wg      *sync.WaitGroup
}

type LogMessage struct {
	Level   rune
	Message string
	Fields  map[string]interface{}
}

func NewLogger(sizeChan int) *Logger {
	return &Logger{
		mu:      &sync.Mutex{},
		logger:  logrus.New(),
		logChan: make(chan LogMessage, sizeChan),
		wg:      &sync.WaitGroup{},
	}
}

func (l *Logger) initialization(nameService, address string) error {

	var err error
	for i := 0; i < 10; i++ {
		l.conn, err = net.Dial("tcp", address)
		if err != nil {
			log.Println("Failed to connect to logstash, retrying...", err.Error())
			time.Sleep(time.Second * 10)
		} else {
			log.Println("Connected to logstash")
			err = nil
			break
		}
	}

	if err != nil {
		return fmt.Errorf("failed to connect to logstash after retries: %w", err)
	}

	hook := logrustash.New(l.conn, logrustash.DefaultFormatter(logrus.Fields{
		"type": nameService,
	}))
	log.Println("Hook created")

	l.logger.Hooks.Add(hook)

	return nil
}

func (l *Logger) error(msg LogMessage) {
	l.mu.Lock()
	l.logger.Level = logrus.ErrorLevel
	l.logger.WithFields(msg.Fields).Error(msg.Message)
	l.mu.Unlock()
}

func (l *Logger) info(msg LogMessage) {
	l.mu.Lock()
	l.logger.Level = logrus.InfoLevel
	l.logger.WithFields(msg.Fields).Info(msg.Message)
	l.mu.Unlock()
}

func (l *Logger) debug(msg LogMessage) {

	l.mu.Lock()
	l.logger.Level = logrus.DebugLevel
	l.logger.WithFields(msg.Fields).Debug(msg.Message)
	l.mu.Unlock()
}

func InitLogger(sizeChan int, nameService, address string) {
	Log = NewLogger(sizeChan)
	if err := Log.initialization(nameService, address); err != nil {
		panic(err)
	}
}

func (l *Logger) Close() {
	close(l.logChan)
	if l.conn != nil {
		l.conn.Close()
	}
}

func (l *Logger) Start(ctx context.Context, countWorker int) {
	for i := 0; i < countWorker; i++ {
		l.wg.Add(1)
		go l.workerSendMsg(ctx)
	}
}

func (l *Logger) workerSendMsg(ctx context.Context) {
	for {
		select {
		case msg := <-l.logChan:
			log.Println("Received message: ", msg)
			if msg.Level == 'E' {
				l.error(msg)
			} else if msg.Level == 'D' {
				l.debug(msg)
			} else if msg.Level == 'I' {
				l.info(msg)
			}
		case <-ctx.Done():
			log.Println("Worker stopped")
			l.wg.Done()
			return
		}
	}
}

func (l *Logger) SendMsg(msg LogMessage) {
	l.logChan <- msg
}
