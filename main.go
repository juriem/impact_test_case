package main


import (
	"github.com/streadway/amqp"
	"fmt"
	"encoding/json"
	"math"
	"os"
	"strconv"
)

const (
	INCOME_QUEUE = "interest-queue"
	OUTCOME_QUEUE = "solved-interest-queue"
)


type InMessage struct {
	Sum int `json:"sum"`
	Days int `json:"days"`
}

type OutMessage struct {
	Sum int `json:"sum"`
	Days int `json:"days"`
	Interest float64 `json:"interest"`
	TotalSum float64 `json:"totalSum"`
	Token string `json:"token"`
}

func main() {

	conn, err := amqp.Dial("amqp://myjar:myjar@impact.ccat.eu")
	defer conn.Close()
	if err != nil {
		panic(err)
	}

	channel, err := conn.Channel()
	if err != nil {
		panic(err)
	}

	// Declare
	incomeQueue, err := channel.QueueDeclare(INCOME_QUEUE, false, true, false, false, nil)
	if err != nil {
		panic(err)
	}
	fmt.Println(incomeQueue)

	outcomeQueue, err := channel.QueueDeclare(OUTCOME_QUEUE, true, false, false, false, nil)
	if err != nil {
		panic(err)
	}
	fmt.Println(outcomeQueue)

	deliveries, err := channel.Consume(incomeQueue.Name, "test-queue", false, false, false, true, nil)
	if err != nil {
		panic(err)
	}

	jobs := make(chan *InMessage)
	results := make(chan *OutMessage)

	go listener(deliveries, jobs)
	go worker(jobs, results, "aurobin")


//	// 594 34165900
	msg := InMessage{Sum:334, Days:277026099}
	jobs <- &msg
	r := <-results
	fmt.Println("Interest: ", fmt.Sprintf("%.2f", r.Interest), "Total:", fmt.Sprintf("%.2f", r.TotalSum), fmt.Sprintf("%.2f", r.Interest + float64(msg.Sum)))

	fmt.Println(1698789657.63 - 1698789657.92)
	os.Exit(1)


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

func listener(deliveries <-chan amqp.Delivery, jobs chan<- *InMessage ) {
	for {
		d := <-deliveries
		if len(d.Body) > 0 {
			inMsg := InMessage{}
			json.Unmarshal(d.Body, &inMsg)
			if inMsg.Days > 0 && inMsg.Sum > 0 {
				jobs <- &inMsg
			}
		}
		d.Ack(true)
	}
}

func worker(jobs <-chan *InMessage, results chan<- *OutMessage, token string) {
	for {

		job := <- jobs
		fmt.Println(job)
		outMsg := OutMessage{}
		outMsg.Sum = job.Sum
		outMsg.Days = job.Days
		interest := calculateInterest(job.Days, job.Sum)
		interest, _ = strconv.ParseFloat(fmt.Sprintf("%.2f", interest), 2)
		totalSum, _ := strconv.ParseFloat(fmt.Sprintf("%.2f", float64(job.Sum) + interest), 2)
 		outMsg.Interest = interest
		outMsg.TotalSum = totalSum
		outMsg.Token = "test"

		results <- &outMsg
	}
}


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
		interest += (float64(sum) * percent) / float64(100)
	}
	return interest
}

func round(num float64) int {
	return int(num + math.Copysign(0.5, num))
}

func toFixed(num float64, precision int) float64  {
	output := math.Pow(10, float64(precision))
	return float64(round(num * output)) / output
}

