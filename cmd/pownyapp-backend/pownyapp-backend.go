package main

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"log"
	"net/http"
	"time"

	"github.com/timehop/apns"
	"gopkg.in/mgo.v2"
	"gopkg.in/mgo.v2/bson"
)

func getYandexLogin(r *http.Request) (login string, e error) {
	auth := r.Header.Get("Authorization")
	if auth == "" {
		return "", fmt.Errorf("no Authorization header")
	}

	client := &http.Client{}

	req, err := http.NewRequest("GET", "https://login.yandex.ru/info?format=json", nil)
	if err != nil {
		return "", err
	}
	req.Header.Add("Authorization", auth)

	resp, err := client.Do(req)
	if err != nil {
		return "", err
	}
	defer func() {
		if err := resp.Body.Close(); err != nil {
			e = err
		}
	}()

	body, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return "", err
	}

	var v struct {
		FirstName   string `json:"first_name"`
		LastName    string `json:"last_name"`
		DisplayName string `json:"display_name"`
		RealName    string `json:"real_name"`
		Login       string `json:"login"`
		Sex         string `json:"sex"`
		ID          string `json:"id"`
	}
	err = json.Unmarshal(body, &v)
	if err != nil {
		return "", err
	}

	return v.Login, nil
}

type Message struct {
	ID                string `json:"id"`
	Severity          string `json:"severity"`
	Subject           string `json:"subject"`
	Details           string `json:"details"`
	TimeKafka         int64  `json:"time_kafka" bson:"time_kafka"`
	TimeMobileBackend int64  `json:"time_mobile_backend" bson:"time_mobile_backend"`
	TimeMobileApp     *int64 `json:"time_mobile_app" bson:"time_mobile_app"`

	Recipient string `json:"-"`
}

func writeResponse(w http.ResponseWriter, status int, response interface{}) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(response)
}

func writeError(w http.ResponseWriter, status int, format string, v ...interface{}) {
	msg := fmt.Sprintf(format, v...)
	log.Printf("error %d: %s", status, msg)
	writeResponse(w, status, map[string]interface{}{
		"status":  "error",
		"message": msg,
	})
}

type Server struct {
	mongoSession *mgo.Session
	apnsClient   apns.Client
}

func (s *Server) putMessage(login string, msg Message) error {
	sess := s.mongoSession.Copy()
	defer sess.Close()

	msg.Recipient = login

	return sess.DB("").C("messages").Insert(msg)
}

func (s *Server) getMessages(login string) ([]Message, error) {
	sess := s.mongoSession.Copy()
	defer sess.Close()

	now := time.Now().Unix()
	_, err := sess.DB("").C("messages").UpdateAll(bson.M{
		"recipient":       login,
		"time_mobile_app": nil,
	}, bson.M{
		"$set": bson.M{
			"time_mobile_app": now,
		},
	})
	if err != nil {
		return nil, err
	}

	var messages []Message
	err = sess.DB("").C("messages").Find(bson.M{
		"recipient":       login,
		"time_mobile_app": bson.M{"$ne": nil},
	}).Sort("-time_mobile_backend").All(&messages)
	return messages, err
}

func (s *Server) deleteMessagesBySubject(login string, subject string) error {
	sess := s.mongoSession.Copy()
	defer sess.Close()

	return sess.DB("").C("messages").Remove(bson.M{
		"recipient": login,
		"subject":   subject,
	})
}

func (s *Server) deleteMessagesByID(login string, id string) error {
	sess := s.mongoSession.Copy()
	defer sess.Close()

	return sess.DB("").C("messages").Remove(bson.M{
		"recipient": login,
		"id":        id,
	})
}

func (s *Server) setIOSToken(login string, token string) error {
	sess := s.mongoSession.Copy()
	defer sess.Close()

	_, err := sess.DB("").C("ios_token").Upsert(bson.M{
		"login": login,
	}, bson.M{
		"$set": bson.M{
			"login": login,
			"token": token,
		},
	})
	return err
}

func (s *Server) getIOSToken(login string) (string, error) {
	sess := s.mongoSession.Copy()
	defer sess.Close()

	var v struct {
		Token string
	}
	err := sess.DB("").C("ios_token").Find(bson.M{
		"login": login,
	}).One(&v)
	return v.Token, err
}

func (s *Server) sendPushNotification(login string) {
	token, err := s.getIOSToken(login)
	if err != nil {
		log.Print("Failed to get token for ", login, ": ", err)
		return
	}

	p := apns.NewPayload()
	//p.APS.Alert.Body = "Received a new message."
	//badge := 1
	//p.APS.Badge = &badge
	//p.APS.Sound = "1"
	p.APS.Sound = ""
	p.APS.ContentAvailable = 1

	m := apns.NewNotification()
	m.Payload = p
	m.DeviceToken = token
	m.Priority = apns.PriorityImmediate
	//m.Identifier = 12312, // Integer for APNS
	//m.ID = "user_id:timestamp", // ID not sent to Apple – to identify error notifications

	err = s.apnsClient.Send(m)
	log.Print("sent push for ", login, ": ", err)
}

func (s *Server) PutMessageHandler(w http.ResponseWriter, r *http.Request) {
	var v struct {
		Recipient string `json:"recipient"`
		Message   struct {
			ID       string `json:"id"`
			Accepted int64  `json:"accepted"`
			Type     string `json:"type"`
			Data     struct {
				Hash    string `json:"hash"`
				Host    string `json:"host"`
				Service string `json:'service"`
				Message struct {
					Description string `json:"description"`
					Status      string `json:"status"`
				} `json:"message"`
				Monitor string `json:"monitor"`
			} `json:"data"`
			Sender struct {
				// Cluster
				IP string `json:"ip"`
				// Project
			} `json:"sender"`
		} `json:"message"`
	}
	err := json.NewDecoder(r.Body).Decode(&v)
	if err != nil {
		writeError(w, 400, "bad JSON: "+err.Error())
		return
	}

	if v.Message.Type != "juggler" {
		writeError(w, 400, "message type should be juggler")
		return
	}

	log.Printf("put: %+v", v)
	msg := Message{
		ID:       v.Message.ID,
		Severity: v.Message.Data.Message.Status,
		Subject:  v.Message.Data.Host + ":" + v.Message.Data.Service,
		Details:  v.Message.Data.Message.Description,

		TimeKafka:         v.Message.Accepted,
		TimeMobileBackend: time.Now().Unix(),
	}

	if v.Recipient == "" {
		writeError(w, 400, "expected non-empty recipient")
		return
	}
	if msg.Severity == "" {
		writeError(w, 400, "expected non-empty message.data.message.status")
		return
	}
	if msg.Severity != "OK" && msg.Severity != "WARN" && msg.Severity != "CRIT" {
		writeError(w, 400, "message status should be OK, WARN or CRIT")
		return
	}
	if msg.Subject == "" {
		writeError(w, 400, "expected non-empty subject")
		return
	}

	err = s.putMessage(v.Recipient, msg)
	if err != nil {
		writeError(w, 500, "failed to save message: "+err.Error())
		return
	}

	writeResponse(w, 200, map[string]interface{}{
		"status":  "success",
		"message": msg,
	})

	s.sendPushNotification(v.Recipient)
}

func (s *Server) GetMessagesHandler(w http.ResponseWriter, r *http.Request) {
	login, err := getYandexLogin(r)
	if login == "" || err != nil {
		writeError(w, 401, "unauthorized")
		return
	}

	messages, err := s.getMessages(login)
	if err != nil {
		writeError(w, 500, "failed to get messages: "+err.Error())
		return
	}

	if messages == nil {
		messages = []Message{}
	}
	writeResponse(w, 200, map[string]interface{}{
		"status":   "success",
		"messages": messages,
	})
}

func (s *Server) DeleteMessageHandler(w http.ResponseWriter, r *http.Request) {
	login, err := getYandexLogin(r)
	if login == "" || err != nil {
		writeError(w, 403, "forbidden")
		return
	}

	subject := r.FormValue("subject")
	if subject != "" {
		err = s.deleteMessagesBySubject(login, subject)
		if err != nil {
			writeError(w, 500, "failed to delete message: "+err.Error())
			return
		}
	}

	id := r.FormValue("id")
	if id != "" {
		err = s.deleteMessagesByID(login, id)
		if err != nil {
			writeError(w, 500, "failed to delete message: "+err.Error())
			return
		}
	}

	writeResponse(w, 200, map[string]interface{}{
		"status": "success",
	})
}

func (s *Server) RegisterIOSTokenHandler(w http.ResponseWriter, r *http.Request) {
	login, err := getYandexLogin(r)
	if login == "" || err != nil {
		writeError(w, 403, "forbidden")
		return
	}

	log.Println(*r)
	token := r.FormValue("token")
	if token == "" {
		writeError(w, 400, "expected non-empty token")
		return
	}

	err = s.setIOSToken(login, token)
	if err != nil {
		writeError(w, 500, "failed to set iOS token: "+err.Error())
		return
	}

	writeResponse(w, 200, map[string]interface{}{
		"status": "success",
	})
}

const (
	apnsCert = "/private/Powny_App_1.pem"
	apnsKey  = "/private/Oleg_Bulatov_Dev_Key.pem"
)

func main() {
	var err error

	server := Server{}

	server.mongoSession, err = mgo.DialWithInfo(&mgo.DialInfo{
		Addrs:    []string{"mongodb"},
		Direct:   false,
		FailFast: true,
		Database: "pownyapp",
		Timeout:  10 * time.Second,
	})
	if err != nil {
		log.Fatal("Unable to open MongoDB session: ", err)
	}

	server.mongoSession.SetPoolLimit(128)
	server.mongoSession.SetSyncTimeout(1 * time.Minute)
	server.mongoSession.SetSocketTimeout(1 * time.Minute)
	server.mongoSession.SetSafe(&mgo.Safe{})

	server.mongoSession.DB("").C("messages").EnsureIndex(mgo.Index{
		Key:        []string{"id"},
		Unique:     true,
		Background: true,
		Sparse:     true,
	})
	if err != nil {
		log.Fatal("Unable to create index messages:id: ", err)
	}

	c, err := apns.NewClientWithFiles(apns.SandboxGateway, apnsCert, apnsKey)
	if err != nil {
		log.Fatal("could not create new client", err.Error())
	}

	go func() {
		for f := range c.FailedNotifs {
			fmt.Println("Notif", f.Notif.ID, "failed with", f.Err.Error())
		}
	}()

	server.apnsClient = c

	http.HandleFunc("/powny/ios_token/", func(w http.ResponseWriter, r *http.Request) {
		if r.Method == "POST" {
			server.RegisterIOSTokenHandler(w, r)
			return
		}
		writeError(w, 405, "method not allowed")
	})
	http.HandleFunc("/powny/", func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/powny/" {
			http.NotFound(w, r)
			return
		}
		if r.Method == "GET" {
			server.GetMessagesHandler(w, r)
			return
		}
		if r.Method == "POST" {
			server.PutMessageHandler(w, r)
			return
		}
		if r.Method == "DELETE" {
			server.DeleteMessageHandler(w, r)
			return
		}
		writeError(w, 405, "method not allowed")
	})
	log.Fatal(http.ListenAndServe(":5000", nil))
}
