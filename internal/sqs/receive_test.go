package sqs

import (
    "encoding/xml"
    "io"
    "net/http"
    "net/http/httptest"
    "net/url"
    "strings"
    "testing"
    "time"
)

func TestTicket022ReceivePopsInSendOrder(t *testing.T) {
    queue := NewManager().GetQueue("ticket022-pop")
    queue.Send("one", nil, 0)
    queue.Send("two", nil, 0)

    got := queue.Receive(2, time.Second, 0)
    if len(got) != 2 {
        t.Fatalf("want 2 messages, got %d", len(got))
    }
    if got[0].Body != "one" || got[1].Body != "two" {
        t.Fatalf("popped out of order: %q then %q", got[0].Body, got[1].Body)
    }
    for _, d := range got {
        if d.ReceiptHandle == "" {
            t.Fatal("checkout must return a receipt handle")
        }
        if d.MD5OfBody != md5Hex(d.Body) {
            t.Fatalf("bad body md5 for %q", d.Body)
        }
        if _, ok := decodeReceiptHandle(d.ReceiptHandle); !ok {
            t.Fatal("receipt handle must be decodable")
        }
    }
    if inFlight := queue.Receive(2, time.Second, 0); len(inFlight) != 0 {
        t.Fatalf("popped messages must stay invisible, got %d", len(inFlight))
    }
}

func TestTicket022VisibilityTimeoutRevealsMessage(t *testing.T) {
    queue := NewManager().GetQueue("ticket022-reveal")
    queue.Send("job", nil, 0)

    first := queue.Receive(1, 150*time.Millisecond, 0)
    if len(first) != 1 || first[0].Body != "job" {
        t.Fatalf("want job on first receive, got %+v", first)
    }
    if immediate := queue.Receive(1, 150*time.Millisecond, 0); len(immediate) != 0 {
        t.Fatal("message in flight must stay invisible")
    }

    redelivered := queue.Receive(1, 150*time.Millisecond, 2*time.Second)
    if len(redelivered) != 1 || redelivered[0].MessageID != first[0].MessageID {
        t.Fatal("visibility timeout must re-reveal the same message")
    }
    if redelivered[0].ApproximateReceiveCount != 2 {
        t.Fatalf("want ApproximateReceiveCount 2, got %d", redelivered[0].ApproximateReceiveCount)
    }
    if redelivered[0].ApproximateFirstReceiveAt != first[0].ApproximateFirstReceiveAt {
        t.Fatal("ApproximateFirstReceiveTimestamp must not change on redelivery")
    }
    if redelivered[0].ReceiptHandle == first[0].ReceiptHandle {
        t.Fatal("redelivery must mint a fresh receipt handle")
    }
}

func TestTicket022LongPollWakesOnSend(t *testing.T) {
    queue := NewManager().GetQueue("ticket022-longpoll")

    done := make(chan []*Delivery, 1)
    start := time.Now()
    go func() {
        done <- queue.Receive(1, 5*time.Second, time.Second)
    }()
    time.Sleep(100 * time.Millisecond)
    queue.Send("wake", nil, 0)

    select {
    case got := <-done:
        if elapsed := time.Since(start); elapsed < 50*time.Millisecond {
            t.Fatalf("long poll returned before the message arrived: %v", elapsed)
        }
        if len(got) != 1 || got[0].Body != "wake" {
            t.Fatalf("want wake, got %+v", got)
        }
    case <-time.After(3 * time.Second):
        t.Fatal("long poll never woke up on send")
    }
}

func TestTicket022LongPollTimesOutWhenEmpty(t *testing.T) {
    queue := NewManager().GetQueue("ticket022-empty")
    start := time.Now()
    got := queue.Receive(1, time.Second, 200*time.Millisecond)
    if elapsed := time.Since(start); elapsed < 150*time.Millisecond {
        t.Fatalf("empty long poll returned early: %v", elapsed)
    }
    if len(got) != 0 {
        t.Fatalf("want no messages, got %+v", got)
    }
}

func TestTicket022DeleteMessage(t *testing.T) {
    queue := NewManager().GetQueue("ticket022-delete")
    queue.Send("consumed", nil, 0)

    got := queue.Receive(1, time.Second, 0)
    if len(got) != 1 {
        t.Fatalf("want 1 message, got %d", len(got))
    }
    if err := queue.DeleteMessage(got[0].ReceiptHandle); err != nil {
        t.Fatalf("delete failed: %v", err)
    }
    if err := queue.DeleteMessage("not-a-handle"); err != ErrMalformedReceiptHandle {
        t.Fatal("garbage receipt handles must be rejected")
    }
    if again := queue.Receive(1, 100*time.Millisecond, time.Second); len(again) != 0 {
        t.Fatal("a deleted message must never be redelivered")
    }
}

func TestTicket022WireReceiveMessage(t *testing.T) {
    httpServer := httptest.NewServer(NewServer(NewManager()))
    defer httpServer.Close()

    created := ticket022Post(t, httpServer, "Action=CreateQueue&QueueName=ticket022-wire")
    queueURL := ticket022QueueURL(t, created)

    sendBody := ticket022Post(t, httpServer,
        "Action=SendMessage&QueueUrl="+url.QueryEscape(queueURL)+"&MessageBody=hello")
    if !strings.Contains(sendBody, "<MessageId>") {
        t.Fatalf("SendMessage response missing MessageId: %s", sendBody)
    }

    received := ticket022Post(t, httpServer,
        "Action=ReceiveMessage&QueueUrl="+url.QueryEscape(queueURL)+
            "&MaxNumberOfMessages=5&WaitTimeSeconds=1&AttributeName.1=All")

    var doc struct {
        Result struct {
            Messages []struct {
                MessageID     string `xml:"MessageId"`
                ReceiptHandle string `xml:"ReceiptHandle"`
                MD5OfBody     string `xml:"MD5OfBody"`
                Body          string `xml:"Body"`
                Attributes    []struct {
                    Name  string `xml:"Name"`
                    Value string `xml:"Value"`
                } `xml:"Attribute"`
            } `xml:"Message"`
        } `xml:"ReceiveMessageResult"`
    }
    if err := xml.Unmarshal([]byte(received), &doc); err != nil {
        t.Fatalf("receive returned bad xml: %v\n%s", err, received)
    }
    if len(doc.Result.Messages) != 1 {
        t.Fatalf("want 1 message, got %d\n%s", len(doc.Result.Messages), received)
    }
    message := doc.Result.Messages[0]
    if message.Body != "hello" {
        t.Fatalf("wrong body: %q", message.Body)
    }
    if message.MessageID == "" || message.ReceiptHandle == "" {
        t.Fatalf("missing ids: %+v", message)
    }
    if message.MD5OfBody != md5Hex("hello") {
        t.Fatalf("wrong MD5OfBody: %s", message.MD5OfBody)
    }
    var senderID string
    for _, attr := range message.Attributes {
        if attr.Name == "SenderId" {
            senderID = attr.Value
        }
    }
    if senderID == "" {
        t.Fatal("AttributeName=All must return SenderId")
    }
}

func TestTicket022WireEmptyReceiveLongPolls(t *testing.T) {
    httpServer := httptest.NewServer(NewServer(NewManager()))
    defer httpServer.Close()

    created := ticket022Post(t, httpServer, "Action=CreateQueue&QueueName=ticket022-wire-empty")
    queueURL := ticket022QueueURL(t, created)

    start := time.Now()
    body := ticket022Post(t, httpServer,
        "Action=ReceiveMessage&QueueUrl="+url.QueryEscape(queueURL)+"&WaitTimeSeconds=1")
    if elapsed := time.Since(start); elapsed < 300*time.Millisecond {
        t.Fatalf("empty long poll returned early after %v", elapsed)
    }
    if !strings.Contains(body, "<ReceiveMessageResult/>") {
        t.Fatalf("empty receive must render an empty result: %s", body)
    }
}

func ticket022Post(t *testing.T, server *httptest.Server, form string) string {
    t.Helper()
    response, err := http.Post(server.URL, "application/x-www-form-urlencoded", strings.NewReader(form))
    if err != nil {
        t.Fatalf("post %q: %v", form, err)
    }
    defer response.Body.Close()
    payload, err := io.ReadAll(response.Body)
    if err != nil {
        t.Fatalf("read body: %v", err)
    }
    return string(payload)
}

func ticket022QueueURL(t *testing.T, doc string) string {
    t.Helper()
    var out struct {
        QueueURL string `xml:"CreateQueueResult>QueueUrl"`
    }
    if err := xml.Unmarshal([]byte(doc), &out); err != nil {
        t.Fatalf("bad CreateQueue xml: %v\n%s", err, doc)
    }
    if out.QueueURL == "" {
        t.Fatalf("CreateQueue returned no QueueUrl: %s", doc)
    }
    return out.QueueURL
}
