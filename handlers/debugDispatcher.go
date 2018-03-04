package handlers

import (
	"bytes"
	"encoding/json"
	"fmt"
	"github.com/Azure/azure-storage-azcopy/common"
	tm "github.com/buger/goterm"
	"io/ioutil"
	"math"
	"net/http"
	"time"
)

type coordinatorScheduleFunc func(*common.CopyJobPartOrder)

func generateCoordinatorScheduleFunc() coordinatorScheduleFunc {
	return func(jobPartOrder *common.CopyJobPartOrder) {
		order, _ := json.MarshalIndent(jobPartOrder, "", "  ")
		sendJobPartOrderToSTE(order)
	}
}

func sendJobPartOrderToSTE(payload []byte) {
	url := "http://localhost:1337"
	payloadContentType := "application/json; charset=utf-8"
	payloadBuffer := bytes.NewBuffer(payload)

	res, err := http.Post(url, payloadContentType, payloadBuffer)
	if err != nil {
		// try a second time after 2 second, in case the transfer engine has not finished booting up
		// TODO this should be smarter after refactoring
		time.Sleep(2 * time.Second)
		res, err = http.Post(url, payloadContentType, payloadBuffer)

		if err != nil {
			panic(err)
		}
	}

	defer res.Body.Close()
	_, err = ioutil.ReadAll(res.Body)
	if err != nil {
		panic(err)
	}
	//fmt.Println("Response to request", res.Status, " ", body)
}

func fetchJobStatus(jobId string) string {
	url := "http://localhost:1337"
	client := &http.Client{}
	req, err := http.NewRequest("GET", url, nil)
	if err != nil {
		panic(err)
	}
	lsCommand := common.ListJobPartsTransfers{JobId: jobId, ExpectedTransferStatus: math.MaxUint8}
	lsCommandMarshalled, err := json.Marshal(lsCommand)
	if err != nil {
		panic(err)
	}
	q := req.URL.Query()
	q.Add("Type", "list")
	q.Add("command", string(lsCommandMarshalled))
	req.URL.RawQuery = q.Encode()

	resp, err := client.Do(req)
	if err != nil {
		panic(err)
	}
	if resp.StatusCode != http.StatusAccepted {
		fmt.Println("request failed with status ", resp.Status)
		panic(err)
	}

	defer resp.Body.Close()
	body, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		panic(err)
	}
	var summary common.JobProgressSummary
	json.Unmarshal(body, &summary)

	tm.Clear()
	tm.MoveCursor(1, 1)

	fmt.Println("----------------- Progress Summary for JobId ", jobId, "------------------")
	tm.Println("Total Number of Transfers: ", summary.TotalNumberOfTransfers)
	tm.Println("Total Number of Transfers Completed: ", summary.TotalNumberofTransferCompleted)
	tm.Println("Total Number of Transfers Failed: ", summary.TotalNumberofFailedTransfer)
	tm.Println("Job order fully received: ", summary.CompleteJobOrdered)

	//tm.Println(tm.Background(tm.Color(tm.Bold(fmt.Sprintf("Job Progress: %d %%", summary.PercentageProgress)), tm.WHITE), tm.GREEN))
	//tm.Println(tm.Background(tm.Color(tm.Bold(fmt.Sprintf("Realtime Throughput: %f MB/s", summary.ThroughputInBytesPerSeconds/1024/1024)), tm.WHITE), tm.BLUE))

	tm.Println(fmt.Sprintf("Job Progress: %d %%", summary.PercentageProgress))
	tm.Println(fmt.Sprintf("Realtime Throughput: %f MB/s", summary.ThroughputInBytesPerSeconds/1024/1024))

	for index := 0; index < len(summary.FailedTransfers); index++ {
		message := fmt.Sprintf("transfer-%d	source: %s	destination: %s", index, summary.FailedTransfers[index].Src, summary.FailedTransfers[index].Dst)
		fmt.Println(message)
	}
	tm.Flush()

	return summary.JobStatus
}
