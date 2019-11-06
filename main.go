package main

import (
	"astuart.co/goq"
	"bytes"
	"encoding/json"
	"fmt"
	"github.com/go-redis/redis"
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
	AssociatedRequestID string `json:"associatedRequestId"`
	ContinuationToken   string `json:"continuationToken"`
	ResultCount         int    `json:"resultCount"`
	Results             []struct {
		Key                string        `json:"key"`
		FullName           string        `json:"fullName"`
		Headline           interface{}   `json:"headline"`
		Description        interface{}   `json:"description"`
		Credential         string        `json:"credential"`
		Designations       []interface{} `json:"designations"`
		StandardRate       interface{}   `json:"standardRate"`
		Location           string        `json:"location"`
		PhotoURL           string        `json:"photoUrl"`
		HasEnhancedProfile bool          `json:"hasEnhancedProfile"`
	} `json:"results"`
}

type Data struct {
	//Table []string `goquery:"table #tabContainer",json:"table"`
	ID                        string
	URL                       string
	Name                      string   `goquery:"div #profileInfo #coachName,text",json:"name"`
	Website                   string   `goquery:"table #tabContainer #webSiteLink ,text",json:"site"`
	Email                     string   `goquery:"table #tabContainer #emailLink ,text",json:"email"`
	Phone                     string   `goquery:"table #tabContainer #phoneLbl ,text",json:"phone"`
	Location                  string   `goquery:"table #tabContainer #addressLbl ,text",json:""`
	CoachingThemes            []string `goquery:"div #detailsTabContent > table > tbody > tr:nth-child(1) > td:nth-child(2)",json:"coaching_themes"`
	CoachingMethods           []string `goquery:"div #detailsTabContent > table > tbody > tr:nth-child(2) > td:nth-child(2)",json:"coaching_methods"`
	Relocate                  string   `goquery:"div #detailsTabContent > table > tbody > tr:nth-child(3) > td:nth-child(2) > div",json:"relocate"`
	SpecialRates              []string `goquery:"div #detailsTabContent > table > tbody > tr:nth-child(4) > td:nth-child(2)",json:"special_rates"`
	FeeRange                  string   `goquery:"div #detailsTabContent > table > tbody > tr:nth-child(5) > td:nth-child(2) > div",json:"fee_range"`
	TypeOfClient              string   `goquery:"div #detailsTabContent > table > tbody > tr:nth-child(6) > td:nth-child(2) > div",json:"type_of_client"`
	OrganizationalClientTypes []string `goquery:"div #detailsTabContent > table > tbody > tr:nth-child(7) > td:nth-child(2)",json:"org_client_types"`
	CoachedOrganizations      []string `goquery:"div #detailsTabContent > table > tbody > tr:nth-child(8) > td:nth-child(2)",json:"coached_orgs"`
	IndustrySectorsCoached    []string `goquery:"div #detailsTabContent > table > tbody > tr:nth-child(9) > td:nth-child(2)",json:"ind_secs_coached"`
	PositionsHeld             []string `goquery:"div #detailsTabContent > table > tbody > tr:nth-child(10) > td:nth-child(2)",json:"pos_held"`
	HasPriorExperience        string   `goquery:"div #detailsTabContent > table > tbody > tr:nth-child(11) > td:nth-child(2) > div",json:"has_prior_exp"`
	Degrees                   []string `goquery:"div #detailsTabContent > table > tbody > tr:nth-child(12) > td:nth-child(2)",json:"degrees"`
	Gender                    string   `goquery:"div #detailsTabContent > table > tbody > tr:nth-child(13) > td:nth-child(2) > div",json:"gender"`
	Age                       string   `goquery:"div #detailsTabContent > table > tbody > tr:nth-child(14) > td:nth-child(2) > div",json:"age"`
	FluentLanguages           []string `goquery:"div #detailsTabContent > table > tbody > tr:nth-child(15) > td:nth-child(2)",json:"languages"`
	CanProvide                []string `goquery:"div #detailsTabContent > table > tbody > tr:nth-child(16) > td:nth-child(2)",json:"can_provide"`
}


func check(e error) {
	if e != nil {
		panic(e)
	}
}


func search(client *redis.Client) {

	var keys []string
	skip := 0
	for i := 1; i <= 8; i++ {
		curl := &http.Client{}
		defer curl.CloseIdleConnections()
		s := strconv.Itoa(skip)
		data := []byte(`{"requestId":"5990252e-de34-48a1-9dc6-9c63e6432c5b","continuationToken":"","skip":` + s + `,"take":"999","sort":"lastName","sortDirection":"asc","keywords":"","filters":{"keywords":"","credentials":[],"services":{"coachingThemes":[],"coachingMethods":{"methods":[],"relocate":false},"standardRate":{"proBono":false,"nonProfitDiscount":false,"feeRanges":[]}},"experience":{"haveCoached":{"clientType":"","organizationalClientTypes":[]},"coachedOrganizations":{"global":false,"nonProfit":false,"industrySector":""},"heldPositions":[]},"demographics":{"gender":"","ageRange":"","fluentLanguages":[],"locations":{"countries":["BC4B70F8-280E-4BB0-B935-9F728C50E183"],"states":[]}},"additional":{"canProvide":[],"designations":[]}}}`)
		resp, err := curl.Post("https://icf-ccf.azurewebsites.net/api/search", "application/x-www-form-urlencoded", bytes.NewBuffer(data))
		check(err)
		body, _ := ioutil.ReadAll(resp.Body)
		var d KeyReponse
		err = json.Unmarshal(body, &d)
		check(err)
		for _, v := range (d.Results) {
			keys = append(keys, v.Key)
			client.RPush("coach_key", v.Key)
		}

		skip = skip + 999
	}

}

//func replace(s []string) []string{
//	var out []string
//	for _, v := range s {
//		v =  strings.ReplaceAll(v, `\\n`, `,`)
//		out = append(out,v)
//	}
//	return  out
//}

var wg sync.WaitGroup

func page(key string, client *redis.Client, session *mgo.Session) {
	defer wg.Done()

	params := fmt.Sprintf("?webcode=ccfcoachprofileview&site=icfapp&coachcstkey=%v", key)
	url := "https://apps.coachfederation.org/eweb/CCFDynamicPage.aspx" + params
	req, err := http.NewRequest("GET", url, nil)
	check(err)

	curl := &http.Client{}
	resp, err := curl.Do(req)
	if err != nil {
		log.Println(err)
		client.RPush("keys_failed", key)
		return
	}
	defer resp.Body.Close()

	log.Println("getting", url)
	log.Println("response Status:", resp.Status)
	if resp.StatusCode != 200 {
		log.Println(key, "failed!")
		client.RPush("keys_failed", key)
		return
	}
	body, _ := ioutil.ReadAll(resp.Body)
	//fmt.Println(string(body))
	var d Data
	err = goq.Unmarshal(body, &d)
	if err != nil {
		client.RPush("keys_failed", key)
		check(err)
	}
	//fmt.Println(d.Website)
	//fmt.Println(d.Email)
	//fmt.Println(d.Phone)
	//fmt.Println(d.Location)
	//fmt.Println(d.CoachingThemes)
	//fmt.Println(d.CoachingMethods)
	//fmt.Println(d.Relocate)
	//fmt.Println(d.SpecialRates)
	//fmt.Println(d.FeeRange)
	//fmt.Println(d.TypeOfClient)
	//fmt.Println(d.OrganizationalClientTypes)
	//fmt.Println(d.CoachedOrganizations)
	//fmt.Println(d.IndustrySectorsCoached)
	//fmt.Println(d.PositionsHeld)
	//fmt.Println(d.HasPriorExperience)
	//fmt.Println(d.Degrees)
	//fmt.Println(d.Gender)
	//fmt.Println(d.Age)
	//fmt.Println(d.FluentLanguages)
	//fmt.Println(d.CanProvide)

	//
	//d.CoachingThemes = replace(d.CoachingThemes)
	//d.CoachingMethods = replace(d.CoachingMethods)
	//d.SpecialRates = replace(d.SpecialRates)
	//d.OrganizationalClientTypes = replace(d.OrganizationalClientTypes)
	//d.CoachedOrganizations = replace(d.CoachedOrganizations)
	//d.IndustrySectorsCoached = replace(d.IndustrySectorsCoached)
	//d.PositionsHeld = replace(d.PositionsHeld)
	//d.Degrees = replace(d.Degrees)
	//d.FluentLanguages = replace(d.FluentLanguages)
	//d.CanProvide = replace(d.CanProvide)

	d.URL = url
	d.ID = key

	collections := session.DB("upwork").C("coaches")
	_, err = collections.Upsert(bson.M{"id": d.ID}, d)
	if err != nil {
		client.RPush("keys_failed", key)
		check(err)

	}
	client.RPush("pages_done", key)
	//
	//b, err := json.MarshalIndent(d, "", " ")
	//if err != nil {
	//	fmt.Println(err)
	//}
	//fmt.Println(string(b))

}

func main() {

	red, ok := os.LookupEnv("redis")
	if !ok {
		red = "localhost:6379"
	}

	client := redis.NewClient(&redis.Options{
		Addr:         red,
		Password:     "", // no password set
		DB:           0,  // use default DB
		ReadTimeout:  -1,
		MaxConnAge:   time.Millisecond,
		MaxRetries:   10,
		MinIdleConns: 50,
	})

	search(client)

	mongo, ok := os.LookupEnv("mongo")
	if !ok {
		mongo = "mongodb://localhost:27017"
	}
	s, err := mgo.Dial(mongo)
	if err != nil {
		log.Println(err)
	}
	session := s.Copy()

	keys := client.LRange("coach_key", int64(0), int64(-1)).Val()
	counter := 0
	for _, key := range (keys) {
		wg.Add(1)
		go page(key, client, session)
		counter++
		// only 40 goroutines at a time
		if counter >= 40 {
			wg.Wait()
			log.Println("sleeping for 3 seconds")
			time.Sleep(3 * time.Second)
			counter = 0
		}
	}

}
