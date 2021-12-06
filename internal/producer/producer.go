// Package producer sends messages in broker
package producer

// Producer have methods of brokers of messages
type Producer interface {
	Write(ch chan int, amountOfMessages int, message string) error
}
