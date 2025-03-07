package main

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"net"
	"net/http"
	"os"
	"strconv"
	"time"

	"github.com/kelvins/geocoder"
	_ "github.com/lib/pq"
)

// -----------------------
// JSON Record Structures
// -----------------------

type TaxiTripsJsonRecords []struct {
	Trip_id                    string `json:"trip_id"`
	Trip_start_timestamp       string `json:"trip_start_timestamp"`
	Trip_end_timestamp         string `json:"trip_end_timestamp"`
	Pickup_centroid_latitude   string `json:"pickup_centroid_latitude"`
	Pickup_centroid_longitude  string `json:"pickup_centroid_longitude"`
	Dropoff_centroid_latitude  string `json:"dropoff_centroid_latitude"`
	Dropoff_centroid_longitude string `json:"dropoff_centroid_longitude"`
}

type UnemploymentJsonRecords []struct {
	Community_area                             string `json:"community_area"`
	Community_area_name                        string `json:"community_area_name"`
	Birth_rate                                 string `json:"birth_rate"`
	General_fertility_rate                     string `json:"general_fertility_rate"`
	Low_birth_weight                           string `json:"low_birth_weight"`
	Prenatal_care_beginning_in_first_trimester string `json:"prenatal_care_beginning_in_first_trimester"`
	Preterm_births                             string `json:"preterm_births"`
	Teen_birth_rate                            string `json:"teen_birth_rate"`
	Assault_homicide                           string `json:"assault_homicide"`
	Breast_cancer_in_females                   string `json:"breast_cancer_in_females"`
	Cancer_all_sites                           string `json:"cancer_all_sites"`
	Colorectal_cancer                          string `json:"colorectal_cancer"`
	Diabetes_related                           string `json:"diabetes_related"`
	Firearm_related                            string `json:"firearm_related"`
	Infant_mortality_rate                      string `json:"infant_mortality_rate"`
	Lung_cancer                                string `json:"lung_cancer"`
	Prostate_cancer_in_males                   string `json:"prostate_cancer_in_males"`
	Stroke_cerebrovascular_disease             string `json:"stroke_cerebrovascular_disease"`
	Childhood_blood_lead_level_screening       string `json:"childhood_blood_lead_level_screening"`
	Childhood_lead_poisoning                   string `json:"childhood_lead_poisoning"`
	Gonorrhea_in_females                       string `json:"gonorrhea_in_females"`
	Gonorrhea_in_males                         string `json:"gonorrhea_in_males"`
	Tuberculosis                               string `json:"tuberculosis"`
	Below_poverty_level                        string `json:"below_poverty_level"`
	Crowded_housing                            string `json:"crowded_housing"`
	Dependency                                 string `json:"dependency"`
	No_high_school_diploma                     string `json:"no_high_school_diploma"`
	Per_capita_income                          string `json:"per_capita_income"`
	Unemployment                               string `json:"unemployment"`
}

type BuildingPermitsJsonRecords []struct {
	Id                     string  `json:"id"`
	Permit_Code            string  `json:"permit_"`
	Permit_type            string  `json:"permit_type"`
	Review_type            string  `json:"review_type"`
	Application_start_date string  `json:"application_start_date"`
	Issue_date             string  `json:"issue_date"`
	Processing_time        string  `json:"processing_time"`
	Street_number          string  `json:"street_number"`
	Street_direction       string  `json:"street_direction"`
	Street_name            string  `json:"street_name"`
	Suffix                 string  `json:"suffix"`
	Work_description       string  `json:"work_description"`
	Building_fee_paid      string  `json:"building_fee_paid"`
	Zoning_fee_paid        string  `json:"zoning_fee_paid"`
	Other_fee_paid         string  `json:"other_fee_paid"`
	Subtotal_paid          string  `json:"subtotal_paid"`
	Building_fee_unpaid    string  `json:"building_fee_unpaid"`
	Zoning_fee_unpaid      string  `json:"zoning_fee_unpaid"`
	Other_fee_unpaid       string  `json:"other_fee_unpaid"`
	Subtotal_unpaid        string  `json:"subtotal_unpaid"`
	Building_fee_waived    string  `json:"building_fee_waived"`
	Zoning_fee_waived      string  `json:"zoning_fee_waived"`
	Other_fee_waived       string  `json:"other_fee_waived"`
	Subtotal_waived        string  `json:"subtotal_waived"`
	Total_fee              string  `json:"total_fee"`
	Contact_1_type         string  `json:"contact_1_type"`
	Contact_1_name         string  `json:"contact_1_name"`
	Contact_1_city         string  `json:"contact_1_city"`
	Contact_1_state        string  `json:"contact_1_state"`
	Contact_1_zipcode      string  `json:"contact_1_zipcode"`
	Reported_cost          string  `json:"reported_cost"`
	Pin1                   string  `json:"pin1"`
	Pin2                   string  `json:"pin2"`
	Community_area         string  `json:"community_area"`
	Census_tract           string  `json:"census_tract"`
	Ward                   string  `json:"ward"`
	Xcoordinate            float64 `json:"xcoordinate"`
	Ycoordinate            float64 `json:"ycoordinate"`
	Latitude               float64 `json:"latitude"`
	Longitude              float64 `json:"longitude"`
}

type CovidJsonRecords []struct {
	Zip_code                           string `json:"zip_code"`
	Week_number                        string `json:"week_number"`
	Week_start                         string `json:"week_start"`
	Week_end                           string `json:"week_end"`
	Cases_weekly                       string `json:"cases_weekly"`
	Cases_cumulative                   string `json:"cases_cumulative"`
	Case_rate_weekly                   string `json:"case_rate_weekly"`
	Case_rate_cumulative               string `json:"case_rate_cumulative"`
	Percent_tested_positive_weekly     string `json:"percent_tested_positive_weekly"`
	Percent_tested_positive_cumulative string `json:"percent_tested_positive_cumulative"`
	Population                         string `json:"population"`
}

type CCVIJsonRecords []struct {
	Geography_type             string `json:"geography_type"`
	Community_area_or_ZIP_code string `json:"community_area_or_zip"`
	Community_name             string `json:"community_area_name"`
	CCVI_score                 string `json:"ccvi_score"`
	CCVI_category              string `json:"ccvi_category"`
}

// -----------------------
// Global Database Variable
// -----------------------

var db *sql.DB

// -----------------------
// Initialization
// -----------------------

func init() {
	var err error
	fmt.Println("Initializing the DB connection")

	// Use your Cloud SQL connection string (update as needed)
	db_connection := "user=postgres dbname=chicago_business_intelligence password=root host=/cloudsql/mineral-rune-453003-e9:us-central1:mypostgres sslmode=disable port=5432"
	db, err = sql.Open("postgres", db_connection)
	if err != nil {
		log.Fatal("Couldn't open connection to database:", err)
	}
	// Optionally test the connection:
	// if err := db.Ping(); err != nil {
	//    log.Fatal("Couldn't connect to database:", err)
	// }
}

//
// MAIN FUNCTION
//
func main() {
	// Set the geocoder API key (ensure this key is kept secure)
	geocoder.ApiKey = "AIzaSyBe9iNcTbNP-cg7UanWrKbDg0NVbXCvG18"

	// Start the HTTP server in a separate goroutine
	go func() {
		http.HandleFunc("/", handler)
		port := os.Getenv("PORT")
		if port == "" {
			port = "8080"
			log.Printf("Defaulting to port %s", port)
		}
		log.Printf("Listening on port %s", port)
		if err := http.ListenAndServe(":"+port, nil); err != nil {
			log.Fatal(err)
		}
	}()

	// Periodically run data collection (every 24 hours)
	for {
		log.Print("Starting CBI Microservices data collection...")
		go GetCommunityAreaUnemployment(db)
		go GetBuildingPermits(db)
		go GetTaxiTrips(db)
		// Uncomment these once implemented:
		// go GetCovidDetails(db)
		// go GetCCVIDetails(db)

		time.Sleep(24 * time.Hour)
	}
}

//
// HTTP Handler for health checks
//
func handler(w http.ResponseWriter, r *http.Request) {
	name := os.Getenv("PROJECT_ID")
	if name == "" {
		name = "CBI-Project"
	}
	fmt.Fprintf(w, "CBI data collection microservices' goroutines have started for %s!\n", name)
}

// -----------------------
// Data Collection Functions
// -----------------------

func GetTaxiTrips(db *sql.DB) {
	fmt.Println("GetTaxiTrips: Collecting Taxi Trips Data")

	// Drop existing table and create a new one
	_, err := db.Exec(`DROP TABLE IF EXISTS taxi_trips`)
	if err != nil {
		panic(err)
	}
	createTable := `CREATE TABLE IF NOT EXISTS taxi_trips (
		id SERIAL,
		trip_id VARCHAR(255) UNIQUE,
		trip_start_timestamp TIMESTAMP WITH TIME ZONE,
		trip_end_timestamp TIMESTAMP WITH TIME ZONE,
		pickup_centroid_latitude DOUBLE PRECISION,
		pickup_centroid_longitude DOUBLE PRECISION,
		dropoff_centroid_latitude DOUBLE PRECISION,
		dropoff_centroid_longitude DOUBLE PRECISION,
		pickup_zip_code VARCHAR(255),
		dropoff_zip_code VARCHAR(255),
		PRIMARY KEY (id)
	);`
	_, err = db.Exec(createTable)
	if err != nil {
		panic(err)
	}
	fmt.Println("Created Table for Taxi Trips")

	// Retrieve Taxi Trips data from the SODA REST API
	url := "https://data.cityofchicago.org/resource/wrvz-psew.json?$limit=500"
	tr := &http.Transport{
		MaxIdleConns:          10,
		IdleConnTimeout:       1000 * time.Second,
		TLSHandshakeTimeout:   1000 * time.Second,
		ExpectContinueTimeout: 1000 * time.Second,
		DisableCompression:    true,
		Dial: (&net.Dialer{
			Timeout:   1000 * time.Second,
			KeepAlive: 1000 * time.Second,
		}).Dial,
		ResponseHeaderTimeout: 1000 * time.Second,
	}
	client := &http.Client{Transport: tr}
	res, err := client.Get(url)
	if err != nil {
		panic(err)
	}
	fmt.Println("Received data from SODA REST API for Taxi Trips")
	body1, err := ioutil.ReadAll(res.Body)
	if err != nil {
		panic(err)
	}
	var taxiTripsList1 TaxiTripsJsonRecords
	if err := json.Unmarshal(body1, &taxiTripsList1); err != nil {
		panic(err)
	}

	// Retrieve rideshare Taxi Trips data
	url2 := "https://data.cityofchicago.org/resource/m6dm-c72p.json?$limit=500"
	res2, err := http.Get(url2)
	if err != nil {
		panic(err)
	}
	fmt.Println("Received data from SODA REST API for Transportation-Network-Providers-Trips")
	body2, err := ioutil.ReadAll(res2.Body)
	if err != nil {
		panic(err)
	}
	var taxiTripsList2 TaxiTripsJsonRecords
	if err := json.Unmarshal(body2, &taxiTripsList2); err != nil {
		panic(err)
	}
	s := fmt.Sprintf("\n\nTransportation-Network-Providers-Trips number of SODA records received = %d\n\n", len(taxiTripsList2))
	io.WriteString(os.Stdout, s)

	// Combine the two datasets
	taxiTripsList := append(taxiTripsList1, taxiTripsList2...)

	// Process and insert records into the database
	for i := 0; i < len(taxiTripsList); i++ {
		tripID := taxiTripsList[i].Trip_id
		if tripID == "" {
			continue
		}
		tripStart := taxiTripsList[i].Trip_start_timestamp
		if len(tripStart) < 23 {
			continue
		}
		tripEnd := taxiTripsList[i].Trip_end_timestamp
		if len(tripEnd) < 23 {
			continue
		}
		pickupLatStr := taxiTripsList[i].Pickup_centroid_latitude
		if pickupLatStr == "" {
			continue
		}
		pickupLongStr := taxiTripsList[i].Pickup_centroid_longitude
		if pickupLongStr == "" {
			continue
		}
		dropoffLatStr := taxiTripsList[i].Dropoff_centroid_latitude
		if dropoffLatStr == "" {
			continue
		}
		dropoffLongStr := taxiTripsList[i].Dropoff_centroid_longitude
		if dropoffLongStr == "" {
			continue
		}

		pickupLat, err := strconv.ParseFloat(pickupLatStr, 64)
		if err != nil {
			continue
		}
		pickupLong, err := strconv.ParseFloat(pickupLongStr, 64)
		if err != nil {
			continue
		}
		pickupLocation := geocoder.Location{Latitude: pickupLat, Longitude: pickupLong}
		// Debug output
		fmt.Println(pickupLocation)
		pickupAddresses, err := geocoder.GeocodingReverse(pickupLocation)
		if err != nil || len(pickupAddresses) == 0 {
			continue
		}
		pickupZip := pickupAddresses[0].PostalCode

		dropoffLat, err := strconv.ParseFloat(dropoffLatStr, 64)
		if err != nil {
			continue
		}
		dropoffLong, err := strconv.ParseFloat(dropoffLongStr, 64)
		if err != nil {
			continue
		}
		dropoffLocation := geocoder.Location{Latitude: dropoffLat, Longitude: dropoffLong}
		dropoffAddresses, err := geocoder.GeocodingReverse(dropoffLocation)
		if err != nil || len(dropoffAddresses) == 0 {
			continue
		}
		dropoffZip := dropoffAddresses[0].PostalCode

		insertSQL := `INSERT INTO taxi_trips (trip_id, trip_start_timestamp, trip_end_timestamp, pickup_centroid_latitude, pickup_centroid_longitude, dropoff_centroid_latitude, dropoff_centroid_longitude, pickup_zip_code, dropoff_zip_code)
		              VALUES($1, $2, $3, $4, $5, $6, $7, $8, $9)`
		_, err = db.Exec(insertSQL, tripID, tripStart, tripEnd, pickupLatStr, pickupLongStr, dropoffLatStr, dropoffLongStr, pickupZip, dropoffZip)
		if err != nil {
			panic(err)
		}
	}
	fmt.Println("Completed Inserting Rows into the TaxiTrips Table")
}

func GetCommunityAreaUnemployment(db *sql.DB) {
	fmt.Println("GetCommunityAreaUnemployment: Collecting Unemployment Rates Data")
	_, err := db.Exec(`DROP TABLE IF EXISTS community_area_unemployment`)
	if err != nil {
		panic(err)
	}
	createTable := `CREATE TABLE IF NOT EXISTS community_area_unemployment (
		id SERIAL,
		community_area VARCHAR(255) UNIQUE,
		community_area_name VARCHAR(255),
		birth_rate VARCHAR(255),
		general_fertility_rate VARCHAR(255),
		low_birth_weight VARCHAR(255),
		prenatal_care_beginning_in_first_trimester VARCHAR(255),
		preterm_births VARCHAR(255),
		teen_birth_rate VARCHAR(255),
		assault_homicide VARCHAR(255),
		breast_cancer_in_females VARCHAR(255),
		cancer_all_sites VARCHAR(255),
		colorectal_cancer VARCHAR(255),
		diabetes_related VARCHAR(255),
		firearm_related VARCHAR(255),
		infant_mortality_rate VARCHAR(255),
		lung_cancer VARCHAR(255),
		prostate_cancer_in_males VARCHAR(255),
		stroke_cerebrovascular_disease VARCHAR(255),
		childhood_blood_lead_level_screening VARCHAR(255),
		childhood_lead_poisoning VARCHAR(255),
		gonorrhea_in_females VARCHAR(255),
		gonorrhea_in_males VARCHAR(255),
		tuberculosis VARCHAR(255),
		below_poverty_level VARCHAR(255),
		crowded_housing VARCHAR(255),
		dependency VARCHAR(255),
		no_high_school_diploma VARCHAR(255),
		unemployment VARCHAR(255),
		per_capita_income VARCHAR(255),
		PRIMARY KEY (id)
	);`
	_, err = db.Exec(createTable)
	if err != nil {
		panic(err)
	}
	fmt.Println("Created Table for community_area_unemployment")
	url := "https://data.cityofchicago.org/resource/iqnk-2tcu.json?$limit=100"
	tr := &http.Transport{
		MaxIdleConns:       10,
		IdleConnTimeout:    300 * time.Second,
		DisableCompression: true,
	}
	client := &http.Client{Transport: tr}
	res, err := client.Get(url)
	if err != nil {
		panic(err)
	}
	fmt.Println("Community Areas Unemployment: Received data from SODA REST API")
	body, err := ioutil.ReadAll(res.Body)
	if err != nil {
		panic(err)
	}
	var unemploymentData UnemploymentJsonRecords
	if err := json.Unmarshal(body, &unemploymentData); err != nil {
		panic(err)
	}
	s := fmt.Sprintf("\n\nCommunity Areas number of SODA records received = %d\n\n", len(unemploymentData))
	io.WriteString(os.Stdout, s)
	for i := 0; i < len(unemploymentData); i++ {
		communityArea := unemploymentData[i].Community_area
		if communityArea == "" {
			continue
		}
		communityAreaName := unemploymentData[i].Community_area_name
		if communityAreaName == "" {
			continue
		}
		// Retrieve other fields as needed
		birthRate := unemploymentData[i].Birth_rate
		generalFertility := unemploymentData[i].General_fertility_rate
		lowBirthWeight := unemploymentData[i].Low_birth_weight
		prenatalCare := unemploymentData[i].Prenatal_care_beginning_in_first_trimester
		pretermBirths := unemploymentData[i].Preterm_births
		teenBirthRate := unemploymentData[i].Teen_birth_rate
		assaultHomicide := unemploymentData[i].Assault_homicide
		breastCancer := unemploymentData[i].Breast_cancer_in_females
		cancerAllSites := unemploymentData[i].Cancer_all_sites
		colorectalCancer := unemploymentData[i].Colorectal_cancer
		diabetesRelated := unemploymentData[i].Diabetes_related
		firearmRelated := unemploymentData[i].Firearm_related
		infantMortality := unemploymentData[i].Infant_mortality_rate
		lungCancer := unemploymentData[i].Lung_cancer
		prostateCancer := unemploymentData[i].Prostate_cancer_in_males
		strokeDisease := unemploymentData[i].Stroke_cerebrovascular_disease
		childhoodBloodLead := unemploymentData[i].Childhood_blood_lead_level_screening
		childhoodLeadPoisoning := unemploymentData[i].Childhood_lead_poisoning
		gonorrheaF := unemploymentData[i].Gonorrhea_in_females
		gonorrheaM := unemploymentData[i].Gonorrhea_in_males
		tuberculosis := unemploymentData[i].Tuberculosis
		belowPoverty := unemploymentData[i].Below_poverty_level
		crowdedHousing := unemploymentData[i].Crowded_housing
		dependency := unemploymentData[i].Dependency
		noHighSchoolDiploma := unemploymentData[i].No_high_school_diploma
		unemploymentRate := unemploymentData[i].Unemployment
		perCapitaIncome := unemploymentData[i].Per_capita_income

		insertSQL := `INSERT INTO community_area_unemployment 
			(community_area, community_area_name, birth_rate, general_fertility_rate, low_birth_weight, prenatal_care_beginning_in_first_trimester, preterm_births, teen_birth_rate, assault_homicide, breast_cancer_in_females, cancer_all_sites, colorectal_cancer, diabetes_related, firearm_related, infant_mortality_rate, lung_cancer, prostate_cancer_in_males, stroke_cerebrovascular_disease, childhood_blood_lead_level_screening, childhood_lead_poisoning, gonorrhea_in_females, gonorrhea_in_males, tuberculosis, below_poverty_level, crowded_housing, dependency, no_high_school_diploma, unemployment, per_capita_income)
			VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,$17,$18,$19,$20,$21,$22,$23,$24,$25,$26,$27,$28,$29)`
		_, err = db.Exec(insertSQL, communityArea, communityAreaName, birthRate, generalFertility, lowBirthWeight, prenatalCare, pretermBirths, teenBirthRate, assaultHomicide, breastCancer, cancerAllSites, colorectalCancer, diabetesRelated, firearmRelated, infantMortality, lungCancer, prostateCancer, strokeDisease, childhoodBloodLead, childhoodLeadPoisoning, gonorrheaF, gonorrheaM, tuberculosis, belowPoverty, crowdedHousing, dependency, noHighSchoolDiploma, unemploymentRate, perCapitaIncome)
		if err != nil {
			panic(err)
		}
	}
	fmt.Println("Completed Inserting Rows into the community_area_unemployment Table")
}

func GetBuildingPermits(db *sql.DB) {
	fmt.Println("GetBuildingPermits: Collecting Building Permits Data")
	_, err := db.Exec(`DROP TABLE IF EXISTS building_permits`)
	if err != nil {
		panic(err)
	}
	createTable := `CREATE TABLE IF NOT EXISTS building_permits (
		id SERIAL,
		permit_id VARCHAR(255) UNIQUE,
		permit_code VARCHAR(255),
		permit_type VARCHAR(255),
		review_type VARCHAR(255),
		application_start_date VARCHAR(255),
		issue_date VARCHAR(255),
		processing_time VARCHAR(255),
		street_number VARCHAR(255),
		street_direction VARCHAR(255),
		street_name VARCHAR(255),
		suffix VARCHAR(255),
		work_description TEXT,
		building_fee_paid VARCHAR(255),
		zoning_fee_paid VARCHAR(255),
		other_fee_paid VARCHAR(255),
		subtotal_paid VARCHAR(255),
		building_fee_unpaid VARCHAR(255),
		zoning_fee_unpaid VARCHAR(255),
		other_fee_unpaid VARCHAR(255),
		subtotal_unpaid VARCHAR(255),
		building_fee_waived VARCHAR(255),
		zoning_fee_waived VARCHAR(255),
		other_fee_waived VARCHAR(255),
		subtotal_waived VARCHAR(255),
		total_fee VARCHAR(255),
		contact_1_type VARCHAR(255),
		contact_1_name VARCHAR(255),
		contact_1_city VARCHAR(255),
		contact_1_state VARCHAR(255),
		contact_1_zipcode VARCHAR(255),
		reported_cost VARCHAR(255),
		pin1 VARCHAR(255),
		pin2 VARCHAR(255),
		community_area VARCHAR(255),
		census_tract VARCHAR(255),
		ward VARCHAR(255),
		xcoordinate DOUBLE PRECISION,
		ycoordinate DOUBLE PRECISION,
		latitude DOUBLE PRECISION,
		longitude DOUBLE PRECISION,
		PRIMARY KEY (id)
	);`
	_, err = db.Exec(createTable)
	if err != nil {
		panic(err)
	}
	fmt.Println("Created Table for Building Permits")
	url := "https://data.cityofchicago.org/resource/building-permits.json?$limit=500"
	tr := &http.Transport{
		MaxIdleConns:       10,
		IdleConnTimeout:    300 * time.Second,
		DisableCompression: true,
	}
	client := &http.Client{Transport: tr}
	res, err := client.Get(url)
	if err != nil {
		panic(err)
	}
	fmt.Println("Received data from SODA REST API for Building Permits")
	body, err := ioutil.ReadAll(res.Body)
	if err != nil {
		panic(err)
	}
	var buildingData BuildingPermitsJsonRecords
	if err := json.Unmarshal(body, &buildingData); err != nil {
		panic(err)
	}
	s := fmt.Sprintf("\n\nBuilding Permits: number of SODA records received = %d\n\n", len(buildingData))
	io.WriteString(os.Stdout, s)
	for i := 0; i < len(buildingData); i++ {
		permitID := buildingData[i].Id
		if permitID == "" {
			continue
		}
		permitCode := buildingData[i].Permit_Code
		if permitCode == "" {
			continue
		}
		permitType := buildingData[i].Permit_type
		if permitType == "" {
			continue
		}
		reviewType := buildingData[i].Review_type
		if reviewType == "" {
			continue
		}
		applicationStart := buildingData[i].Application_start_date
		if applicationStart == "" {
			continue
		}
		issueDate := buildingData[i].Issue_date
		if issueDate == "" {
			continue
		}
		processingTime := buildingData[i].Processing_time
		if processingTime == "" {
			continue
		}
		streetNumber := buildingData[i].Street_number
		if streetNumber == "" {
			continue
		}
		streetDirection := buildingData[i].Street_direction
		if streetDirection == "" {
			continue
		}
		streetName := buildingData[i].Street_name
		if streetName == "" {
			continue
		}
		suffix := buildingData[i].Suffix
		if suffix == "" {
			continue
		}
		workDesc := buildingData[i].Work_description
		if workDesc == "" {
			continue
		}
		buildingFeePaid := buildingData[i].Building_fee_paid
		if buildingFeePaid == "" {
			continue
		}
		zoningFeePaid := buildingData[i].Zoning_fee_paid
		if zoningFeePaid == "" {
			continue
		}
		otherFeePaid := buildingData[i].Other_fee_paid
		if otherFeePaid == "" {
			continue
		}
		subtotalPaid := buildingData[i].Subtotal_paid
		if subtotalPaid == "" {
			continue
		}
		buildingFeeUnpaid := buildingData[i].Building_fee_unpaid
		if buildingFeeUnpaid == "" {
			continue
		}
		zoningFeeUnpaid := buildingData[i].Zoning_fee_unpaid
		if zoningFeeUnpaid == "" {
			continue
		}
		otherFeeUnpaid := buildingData[i].Other_fee_unpaid
		if otherFeeUnpaid == "" {
			continue
		}
		subtotalUnpaid := buildingData[i].Subtotal_unpaid
		if subtotalUnpaid == "" {
			continue
		}
		buildingFeeWaived := buildingData[i].Building_fee_waived
		if buildingFeeWaived == "" {
			continue
		}
		zoningFeeWaived := buildingData[i].Zoning_fee_waived
		if zoningFeeWaived == "" {
			continue
		}
		otherFeeWaived := buildingData[i].Other_fee_waived
		if otherFeeWaived == "" {
			continue
		}
		subtotalWaived := buildingData[i].Subtotal_waived
		if subtotalWaived == "" {
			continue
		}
		totalFee := buildingData[i].Total_fee
		if totalFee == "" {
			continue
		}
		contact1Type := buildingData[i].Contact_1_type
		if contact1Type == "" {
			continue
		}
		contact1Name := buildingData[i].Contact_1_name
		if contact1Name == "" {
			continue
		}
		contact1City := buildingData[i].Contact_1_city
		if contact1City == "" {
			continue
		}
		contact1State := buildingData[i].Contact_1_state
		if contact1State == "" {
			continue
		}
		contact1Zip := buildingData[i].Contact_1_zipcode
		if contact1Zip == "" {
			continue
		}
		reportedCost := buildingData[i].Reported_cost
		if reportedCost == "" {
			continue
		}
		pin1 := buildingData[i].Pin1
		if pin1 == "" {
			continue
		}
		pin2 := buildingData[i].Pin2
		communityArea := buildingData[i].Community_area
		censusTract := buildingData[i].Census_tract
		if censusTract == "" {
			continue
		}
		ward := buildingData[i].Ward
		if ward == "" {
			continue
		}
		xcoord := buildingData[i].Xcoordinate
		ycoord := buildingData[i].Ycoordinate
		lat := buildingData[i].Latitude
		if lat == 0 {
			continue
		}
		lng := buildingData[i].Longitude
		if lng == 0 {
			continue
		}

		insertSQL := `INSERT INTO building_permits 
			(permit_id, permit_code, permit_type, review_type, application_start_date, issue_date, processing_time, street_number, street_direction, street_name, suffix, work_description, building_fee_paid, zoning_fee_paid, other_fee_paid, subtotal_paid, building_fee_unpaid, zoning_fee_unpaid, other_fee_unpaid, subtotal_unpaid, building_fee_waived, zoning_fee_waived, other_fee_waived, subtotal_waived, total_fee, contact_1_type, contact_1_name, contact_1_city, contact_1_state, contact_1_zipcode, reported_cost, pin1, pin2, community_area, census_tract, ward, xcoordinate, ycoordinate, latitude, longitude)
			VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,$17,$18,$19,$20,$21,$22,$23,$24,$25,$26,$27,$28,$29,$30,$31,$32,$33,$34,$35,$36,$37,$38,$39,$40)`
		_, err = db.Exec(insertSQL, permitID, permitCode, permitType, reviewType, applicationStart, issueDate, processingTime, streetNumber, streetDirection, streetName, suffix, workDesc, buildingFeePaid, zoningFeePaid, otherFeePaid, subtotalPaid, buildingFeeUnpaid, zoningFeeUnpaid, otherFeeUnpaid, subtotalUnpaid, buildingFeeWaived, zoningFeeWaived, otherFeeWaived, subtotalWaived, totalFee, contact1Type, contact1Name, contact1City, contact1State, contact1Zip, reportedCost, pin1, pin2, communityArea, censusTract, ward, xcoord, ycoord, lat, lng)
		if err != nil {
			panic(err)
		}
	}
	fmt.Println("Completed Inserting Rows into the Building Permits Table")
}

func GetCovidDetails(db *sql.DB) {
	fmt.Println("GetCovidDetails: To be implemented")
}

func GetCCVIDetails(db *sql.DB) {
	fmt.Println("GetCCVIDetails: To be implemented")
}
