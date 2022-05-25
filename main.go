package main

import (
	"fmt"
	"strings"
	"strconv"
	"os"
	"bytes"
	"encoding/json"
	"net/http"
	"time"
	"io/ioutil"
	"sync"						// Wait to Goroutines

	"github.com/gocarina/gocsv" // Parse to CSV File
)

type Customer struct {
	Email      string `csv:"email"    json:"email"`
	Text       string `csv:"text"     json:"text"`
	Schedule   string `csv:"schedule" json:"schedule"`
	Paid	   bool   `csv::"-"       json:"paid"`
}

type ReminderRequest struct {
	Uri string
	Payload string
	Sleeptime int
}

func GetCSV (csv_file_path string) []*Customer {

	csvFile, err := os.OpenFile(csv_file_path, os.O_RDWR|os.O_CREATE, os.ModePerm)

	if err != nil {
		panic(err)
	}
	defer csvFile.Close()

	customers := []*Customer{}

	if err := gocsv.UnmarshalFile(csvFile, &customers); err != nil { // Load clients from file
		panic(err)
	}

	return customers
}

func PostRequest(request *ReminderRequest, responses map[string]bool, c chan bool, wg *sync.WaitGroup, lock *sync.RWMutex) {
	defer wg.Done()
	
	time.Sleep( time.Second * time.Duration(request.Sleeptime) )

	lock.RLock()
	if responses[request.Payload] == true {
		defer lock.RUnlock()
		c <- true
		return
	}
	lock.RUnlock()

	
	client := http.Client{Timeout: time.Duration(1) * time.Second}
	payload := bytes.NewBuffer([]byte( request.Payload ))
	resp, err := client.Post(request.Uri, "application/json", payload)
	
	if err != nil {
		fmt.Errorf("Error %s", err)
		return
	}
	defer resp.Body.Close()

	body, err := ioutil.ReadAll(resp.Body)
	fmt.Printf("Reponse Status Code: %d, Body : %s", resp.StatusCode, body)

	// Unmarshall response
	var response Customer
	json.Unmarshal([]byte(body), &response)
	
	c <- true
	
	lock.Lock()
	defer lock.Unlock()
	responses[request.Payload] = response.Paid
}

func main() {

	customers := GetCSV("customers.csv")

	requests := []*ReminderRequest {}
	var responses = make(map[string]bool)
	var lock = sync.RWMutex{} // for protecting map object concurrent to read/write

	for _, customer := range customers {
		schedule := customer.Schedule
		data, _ := json.Marshal(customer)
		
		for _, value := range strings.Split(schedule, "-") {	
			sleep_time, err := strconv.Atoi(strings.TrimRight(value, "s"))

			if err != nil {
				panic(err)
			}
			
			_, ok := responses[string(data)]
			if !ok {
				responses[string(data)] = false
			}

			requests = append(requests, &ReminderRequest{Uri:"http://127.0.0.1:9090/messages", Payload:string(data), Sleeptime: sleep_time})
		}
	}

	channel := make(chan bool)

	var wg sync.WaitGroup
	wg.Add(len(requests))

	for i, request := range requests {
		fmt.Printf("[%-2d] Request Payload: %s, Sleeptime %d\n\n", i+1, request.Payload, request.Sleeptime)
		go PostRequest(request, responses, channel, &wg, &lock)

	}

	go func(){
		for response := range channel {
			fmt.Println(response)
		}
	}()
	
	wg.Wait()

	fmt.Println("Processes are done!")
}