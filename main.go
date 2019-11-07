package main

import (
	"astuart.co/goq"
	"bytes"
	"encoding/json"
	"fmt"
	"gopkg.in/mgo.v2"
	"gopkg.in/mgo.v2/bson"
	"io/ioutil"
	"log"
	"net/http"
	"os"
	"strconv"
	"sync"
	"time"
)

type KeyReponse struct {
	Results []struct {
		Key string `json:"key"`
	} `json:"results"`
}

type Data struct {
	ID                        string
	URL                       string
	Name                      string   `goquery:"div #profileInfo #coachName,text"`
	Website                   string   `goquery:"table #tabContainer #webSiteLink ,text"`
	Email                     string   `goquery:"table #tabContainer #emailLink ,text"`
	Phone                     string   `goquery:"table #tabContainer #phoneLbl ,text"`
	Location                  string   `goquery:"table #tabContainer #addressLbl ,text"`
	CoachingThemes            []string `goquery:"div #detailsTabContent > table > tbody > tr:nth-child(1) > td:nth-child(2)"`
	CoachingMethods           []string `goquery:"div #detailsTabContent > table > tbody > tr:nth-child(2) > td:nth-child(2)"`
	Relocate                  string   `goquery:"div #detailsTabContent > table > tbody > tr:nth-child(3) > td:nth-child(2) > div"`
	SpecialRates              []string `goquery:"div #detailsTabContent > table > tbody > tr:nth-child(4) > td:nth-child(2)"`
	FeeRange                  string   `goquery:"div #detailsTabContent > table > tbody > tr:nth-child(5) > td:nth-child(2) > div"`
	TypeOfClient              string   `goquery:"div #detailsTabContent > table > tbody > tr:nth-child(6) > td:nth-child(2) > div"`
	OrganizationalClientTypes []string `goquery:"div #detailsTabContent > table > tbody > tr:nth-child(7) > td:nth-child(2)"`
	CoachedOrganizations      []string `goquery:"div #detailsTabContent > table > tbody > tr:nth-child(8) > td:nth-child(2)"`
	IndustrySectorsCoached    []string `goquery:"div #detailsTabContent > table > tbody > tr:nth-child(9) > td:nth-child(2)"`
	PositionsHeld             []string `goquery:"div #detailsTabContent > table > tbody > tr:nth-child(10) > td:nth-child(2)"`
	HasPriorExperience        string   `goquery:"div #detailsTabContent > table > tbody > tr:nth-child(11) > td:nth-child(2) > div"`
	Degrees                   []string `goquery:"div #detailsTabContent > table > tbody > tr:nth-child(12) > td:nth-child(2)"`
	Gender                    string   `goquery:"div #detailsTabContent > table > tbody > tr:nth-child(13) > td:nth-child(2) > div"`
	Age                       string   `goquery:"div #detailsTabContent > table > tbody > tr:nth-child(14) > td:nth-child(2) > div"`
	FluentLanguages           []string `goquery:"div #detailsTabContent > table > tbody > tr:nth-child(15) > td:nth-child(2)"`
	CanProvide                []string `goquery:"div #detailsTabContent > table > tbody > tr:nth-child(16) > td:nth-child(2)"`
}

var (
	wg      sync.WaitGroup
	mu      sync.Mutex
	result  csvSlice
)

type SafeCounter struct {
	v   map[string]int
	mux sync.Mutex
}

type csvSlice struct {
	v   []Data
	mux sync.Mutex
}

func (d *csvSlice) Add(data Data) {
	d.mux.Lock()
	d.v = append(d.v, data)
	d.mux.Unlock()

}

// Inc increments the counter for the given key.
func (c *SafeCounter) Inc(key string) {
	c.mux.Lock()
	// Lock so only one goroutine at a time can access the map c.v.
	c.v[key]++
	c.mux.Unlock()
}

// Value returns the current value of the counter for the given key.
func (c *SafeCounter) Value(key string) int {
	c.mux.Lock()
	// Lock so only one goroutine at a time can access the map c.v.
	defer c.mux.Unlock()
	return c.v[key]
}

// Value returns the current value of the counter for the given key.
func (c *SafeCounter) Clear(key string) {
	c.mux.Lock()
	// Lock so only one goroutine at a time can access the map c.v.
	c.v[key] = 0
	c.mux.Unlock()
}

func check(e error) {
	if e != nil {
		log.Fatal(e)
	}
}

func search() []string {

	var keys []string
	skip := 0
	for i := 1; i <= 8; i++ {
		curl := &http.Client{}
		s := strconv.Itoa(skip)
		data := []byte(`{"requestId":"5990252e-de34-48a1-9dc6-9c63e6432c5b","continuationToken":"","skip":` + s + `,"take":"999","sort":"lastName","sortDirection":"asc","keywords":"","filters":{"keywords":"","credentials":[],"services":{"coachingThemes":[],"coachingMethods":{"methods":[],"relocate":false},"standardRate":{"proBono":false,"nonProfitDiscount":false,"feeRanges":[]}},"experience":{"haveCoached":{"clientType":"","organizationalClientTypes":[]},"coachedOrganizations":{"global":false,"nonProfit":false,"industrySector":""},"heldPositions":[]},"demographics":{"gender":"","ageRange":"","fluentLanguages":[],"locations":{"countries":["BC4B70F8-280E-4BB0-B935-9F728C50E183"],"states":[]}},"additional":{"canProvide":[],"designations":[]}}}`)
		resp, err := curl.Post("https://icf-ccf.azurewebsites.net/api/search", "application/x-www-form-urlencoded", bytes.NewBuffer(data))
		check(err)
		body, _ := ioutil.ReadAll(resp.Body)
		var d KeyReponse
		err = json.Unmarshal(body, &d)
		check(err)
		for _, v := range d.Results {
			keys = append(keys, v.Key)
		}
		skip = skip + 999
	}
	return keys

}

func page(key string, session *mgo.Session) {
	defer wg.Done()

	params := fmt.Sprintf("?webcode=ccfcoachprofileview&site=icfapp&coachcstkey=%v", key)
	url := "https://apps.coachfederation.org/eweb/CCFDynamicPage.aspx" + params
	req, err := http.NewRequest("GET", url, nil)
	check(err)

	curl := &http.Client{}
	resp, err := curl.Do(req)
	if err != nil {
		log.Println(err)
		return
	}
	defer resp.Body.Close()

	log.Println("getting", url)
	log.Println("response Status:", resp.Status)
	if resp.StatusCode != 200 {
		log.Println(key, "failed!")
		return
	}
	body, _ := ioutil.ReadAll(resp.Body)
	//fmt.Println(string(body))
	var d Data
	err = goq.Unmarshal(body, &d)
	if err != nil {
		check(err)
	}

	d.URL = url
	d.ID = key

	collections := session.DB("upwork").C("coaches_no_redis")
	_, err = collections.Upsert(bson.M{"id": d.ID}, d)
	if err != nil {
		check(err)

	}
	result.Add(d)
	mu.Lock()
	mu.Unlock()
}

func main() {

	mongo, ok := os.LookupEnv("mongo")
	if !ok {
		mongo = "mongodb://localhost:27017"
	}
	s, err := mgo.Dial(mongo)
	if err != nil {
		log.Println(err)
	}
	session := s.Copy()

	keys := search()

	c := SafeCounter{v: make(map[string]int)}
	for _, key := range keys {
		wg.Add(1)
		go page(key, session)
		c.Inc("counter")
		// only 40 goroutines at a time
		if c.Value("counter") >= 40 {
			wg.Wait()
			log.Println("sleeping for 3 seconds")
			time.Sleep(3 * time.Second)
			c.Clear("counter")
		}
	}

}
