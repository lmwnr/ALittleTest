package worker

import (
	"MXAntiCheatOffline/agent"
	"bufio"
	"compress/gzip"
	"fmt"
	"time"
)

type antiCheatWorker struct {
	srcBkt    string
	srcPrefix string
	resBkt    string
	resPrefix string
}

var AntiCheatWorkerIns = &antiCheatWorker{
	srcBkt:    "mx-search-log-statis",
	srcPrefix: "sqs_base_folder/mxmain_anticheat_base",
	resBkt:    "mx-multi",
	resPrefix: "anticheat/mxapilog",
}

func (a *antiCheatWorker) GetLogFiles() ([]string, error) {
	d := time.Now().Add(-1 * 24 * time.Hour)
	dateStr := fmt.Sprintf("%d%02d%02d", d.UTC().Year(), d.UTC().Month(), d.UTC().Day())

	return agent.AwsS3GetFileList(a.srcBkt, fmt.Sprintf("%s/%s", a.srcPrefix, dateStr))
}

func (a *antiCheatWorker) Do() error {
	objs, err := a.GetLogFiles()
	if err != nil {
		return err
	}

	for _, key := range objs {
		err = a.process(key)
		if err != nil {
			return err
		}
	}

	return nil
}

func (a *antiCheatWorker) process(key string) error {
	rd, err := agent.AwsS3GetObjReader(a.srcBkt, key)
	if err != nil {
		return err
	}
	defer rd.Close()

	grd, err := gzip.NewReader(rd)
	if err != nil {
		return err
	}

	sc := bufio.NewScanner(grd)
	for sc.Scan() {
		evtSource := sc.Text()
		fmt.Println(evtSource)
	}

	return nil
}
