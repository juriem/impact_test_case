package main


import (
	"github.com/streadway/amqp"
	"fmt"
	"encoding/json"
	"strconv"
)

const (
	INCOME_QUEUE = "interest-queue"
	OUTCOME_QUEUE = "solved-interest-queue"
)

// Connection
type ConnectionConfig struct {
	user, password string
	host string
}
//
func (config *ConnectionConfig) connect() *amqp.Connection {
	url := fmt.Sprintf("amqp://%s:%s@%s", config.user, config.password, config.host)
	conn, err := amqp.Dial(url)
	if err != nil {
		panic(err)
	}
	return conn
}

// Income message
type InMessage struct {
	Sum int `json:"sum"`
	Days int `json:"days"`
}

// Outcome message
type OutMessage struct {
	Sum int `json:"sum"`
	Days int `json:"days"`
	Interest float64 `json:"interest"`
	TotalSum float64 `json:"totalSum"`
	Token string `json:"token"`
}

func main() {

	connection := &ConnectionConfig{user: "myjar", password: "myjar", host: "impact.ccat.eu"}
	conn := connection.connect()

	channel, err := conn.Channel()
	if err != nil {
		panic(err)
	}

	defer func() {
		channel.Close()
		conn.Close()
	}()

	// Declare income queue
	incomeQueue, err := channel.QueueDeclare(INCOME_QUEUE, false, true, false, false, nil)
	if err != nil {
		panic(err)
	}

	// Declare output queue
	outcomeQueue, err := channel.QueueDeclare(OUTCOME_QUEUE, true, false, false, false, nil)
	if err != nil {
		panic(err)
	}

	// Start consume data
	deliveries, err := channel.Consume(incomeQueue.Name, "", false, false, false, true, nil)
	if err != nil {
		panic(err)
	}

	jobs := make(chan *InMessage)
	results := make(chan *OutMessage)

	go listener(deliveries, jobs)
	go worker(jobs, results, "j.em")

	for {
		outMsg := <-results
		jsonOutMsg, err := json.Marshal(outMsg)
		if err != nil {
			fmt.Println(err)
		}
		if err := channel.Publish("", outcomeQueue.Name, false, false, amqp.Publishing{
			Headers: amqp.Table{},
			ContentType: "application/json",
			ContentEncoding: "utf-8",
			DeliveryMode: amqp.Transient,
			Priority: 0,
			Body: jsonOutMsg,
		}); err != nil {
			fmt.Println(err)
		}
	}
}

// Income data listener
func listener(deliveries <-chan amqp.Delivery, jobs chan<- *InMessage ) {
	for {
		d := <-deliveries
		if len(d.Body) > 0 {
			inMsg := InMessage{}
			json.Unmarshal(d.Body, &inMsg)
			// Allow only normal data: not zero or negative
			if inMsg.Days > 0 && inMsg.Sum > 0 {
				jobs <- &inMsg
			}
		}
		d.Ack(true)
	}
}

// Worker for processing data
func worker(jobs <-chan *InMessage, results chan<- *OutMessage, token string) {
	for {
		job := <- jobs
		
		outMsg := OutMessage{}
		outMsg.Sum = job.Sum
		outMsg.Days = job.Days
		interest := calculateInterest(job.Days, job.Sum) / 100
		interest, _ = strconv.ParseFloat(fmt.Sprintf("%.2f", interest), 2)
		totalSum, _ := strconv.ParseFloat(fmt.Sprintf("%.2f", float64(job.Sum) + interest), 2)
 		outMsg.Interest = interest
		outMsg.TotalSum = totalSum
		outMsg.Token = token

		results <- &outMsg
	}
}

// Calculator
func calculateInterest(days int, sum int) float64 {
	var percent float64
	interest := float64(0)
	for day := 1; day <= days; day++ {
		if day % 3 == 0 && day % 5 == 0 {
			percent = float64(3)
		} else if day % 3 == 0 {
			percent = float64(1)
		} else if day % 5 == 0 {
			percent = float64(2)
		} else {
			percent = float64(4)
		}
		interest += float64(sum) * percent
	}
	return interest
}

