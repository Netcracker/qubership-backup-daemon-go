//go:build ignore

// Copyright 2024-2025 NetCracker Technology Corporation
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package main

import (
	"bytes"
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"

	"github.com/Netcracker/qubership-backup-daemon-go/backup-daemon/app/utils"
	"strconv"
	"strings"
	"time"
)

const (
	iterationTimeout = 5 * time.Second
	programName      = "bdcli"
)

type cliArgs struct {
	command     string
	positional  string
	username    string
	password    string
	host        string
	verify      string
	incremental bool
	verbose     bool
	wait        bool
	timeout     int
	dbs         []string
	properties  map[string]string
	input       string
	output      string
	help        bool
}

type backupClient struct {
	host    string
	auth    *[2]string
	dbs     []interface{}
	props   map[string]string
	wait    bool
	timeout time.Duration
	verbose bool
	client  *http.Client
}

func newBackupClient(host, username, password, verify string,
	dbs []string, props map[string]string,
	wait, verbose bool, timeout int, incremental bool,
) *backupClient {
	if host == "" {
		if os.Getenv("TLS_ENABLED") == "true" {
			host = "https://localhost:8443"
		} else {
			host = "http://localhost:8080"
		}
	}
	if incremental {
		host += "/incremental"
	}

	if username == "" {
		username = utils.GetSecretFromFileOrEnv("BACKUP_DAEMON_API_CREDENTIALS_USERNAME")
	}
	if password == "" {
		password = utils.GetSecretFromFileOrEnv("BACKUP_DAEMON_API_CREDENTIALS_PASSWORD")
	}

	var auth *[2]string
	if username != "" && password != "" {
		auth = &[2]string{username, password}
	}

	if verify == "" {
		if cp := os.Getenv("CERTS_PATH"); cp != "" {
			verify = cp + "/ca.crt"
		}
	}

	tlsCfg := &tls.Config{}
	if strings.EqualFold(verify, "false") {
		tlsCfg.InsecureSkipVerify = true
	} else if verify != "" {
		if caCert, err := os.ReadFile(verify); err == nil {
			pool := x509.NewCertPool()
			pool.AppendCertsFromPEM(caCert)
			tlsCfg.RootCAs = pool
		}
	}

	var parsedDbs []interface{}
	for _, db := range dbs {
		parsedDbs = append(parsedDbs, parseDb(db))
	}

	return &backupClient{
		host:    host,
		auth:    auth,
		dbs:     parsedDbs,
		props:   props,
		wait:    wait,
		timeout: time.Duration(timeout) * time.Second,
		verbose: verbose,
		client:  &http.Client{Transport: &http.Transport{TLSClientConfig: tlsCfg}},
	}
}

func parseDb(db string) interface{} {
	var result interface{}
	if err := json.Unmarshal([]byte(db), &result); err != nil {
		return db
	}
	return result
}

func (bc *backupClient) log(format string, args ...interface{}) {
	if bc.verbose {
		fmt.Printf(format+"\n", args...)
	}
}

func (bc *backupClient) buildBody() map[string]interface{} {
	body := make(map[string]interface{})
	if len(bc.dbs) > 0 {
		body["dbs"] = bc.dbs
	}
	for k, v := range bc.props {
		body[k] = v
	}
	return body
}

func (bc *backupClient) doRequest(method, url string, body map[string]interface{}) (int, string, error) {
	var reqBody io.Reader
	if body != nil {
		data, err := json.Marshal(body)
		if err != nil {
			return 0, "", fmt.Errorf("failed to marshal body: %w", err)
		}
		reqBody = bytes.NewReader(data)
	}

	req, err := http.NewRequest(method, url, reqBody)
	if err != nil {
		return 0, "", fmt.Errorf("failed to create request: %w", err)
	}
	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	if bc.auth != nil {
		req.SetBasicAuth(bc.auth[0], bc.auth[1])
	}

	bc.log("Request: %s %s", method, url)
	if body != nil {
		if data, err := json.Marshal(body); err == nil {
			bc.log("Body: %s", string(data))
		}
	}

	resp, err := bc.client.Do(req)
	if err != nil {
		return 0, "", fmt.Errorf("request failed: %w", err)
	}
	defer resp.Body.Close()

	respBody, err := io.ReadAll(resp.Body)
	if err != nil {
		return resp.StatusCode, "", fmt.Errorf("failed to read response: %w", err)
	}

	bc.log("Response: Status-Code: %d, Content: %s", resp.StatusCode, string(respBody))
	return resp.StatusCode, string(respBody), nil
}

func (bc *backupClient) retryStatus(jobID string) error {
	bc.log("Try to get status for job_id '%s'", jobID)
	start := time.Now()
	for {
		status, err := bc.getStatus(jobID)
		if err != nil {
			return err
		}
		bc.log("Status for '%s' is '%s'", jobID, status)

		var resp map[string]interface{}
		if err := json.Unmarshal([]byte(status), &resp); err == nil {
			if resp["status"] == "Successful" {
				return nil
			}
		}

		if time.Since(start) >= bc.timeout {
			bc.log("Timeout reached. Exiting.")
			return fmt.Errorf("%s", status)
		}

		bc.log("Attempt failed. Retrying in %v seconds...", iterationTimeout.Seconds())
		time.Sleep(iterationTimeout)
	}
}

func (bc *backupClient) performBackup() (string, error) {
	bc.log("Start backup process")
	code, text, err := bc.doRequest("POST", bc.host+"/backup", bc.buildBody())
	if err != nil {
		return "", err
	}
	if code != 200 {
		bc.log("Error executing request, Response: Status-Code: %d, Content: %s", code, text)
		return "", fmt.Errorf("%s", text)
	}
	if bc.wait {
		if err := bc.retryStatus(text); err != nil {
			return "", err
		}
	}
	return text, nil
}

func (bc *backupClient) performRestore(backupID string) (string, error) {
	if backupID == "" {
		return "", fmt.Errorf("Restore cannot be run with empty <backup_id> or <timestamp>")
	}
	bc.log("Start restore process for backup_id '%s'", backupID)
	body := bc.buildBody()
	if isValidTimestamp(backupID) {
		body["ts"] = backupID
	} else {
		body["vault"] = backupID
	}
	code, text, err := bc.doRequest("POST", bc.host+"/restore", body)
	if err != nil {
		return "", err
	}
	if code != 200 {
		bc.log("Error executing request, Response: Status-Code: %d, Content: %s", code, text)
		return "", fmt.Errorf("%s", text)
	}
	if bc.wait {
		if err := bc.retryStatus(text); err != nil {
			return "", err
		}
	}
	return text, nil
}

func (bc *backupClient) performEvict(backupID string) (string, error) {
	url := bc.host + "/evict"
	if backupID != "" {
		url += "/" + backupID
	}
	code, text, err := bc.doRequest("POST", url, nil)
	if err != nil {
		return "", err
	}
	if code != 200 {
		bc.log("Error executing request, Response: Status-Code: %d, Content: %s", code, text)
		return "", fmt.Errorf("%s", text)
	}
	return text, nil
}

func (bc *backupClient) getBackupList() (string, error) {
	bc.log("Getting backup list...")
	code, text, err := bc.doRequest("GET", bc.host+"/listbackups", nil)
	if err != nil {
		return "", err
	}
	if code != 200 {
		bc.log("Error executing request, Response: Status-Code: %d, Content: %s", code, text)
		return "", fmt.Errorf("%s", text)
	}
	return text, nil
}

func (bc *backupClient) describeBackup(backupID string) (string, error) {
	if backupID == "" {
		return "", fmt.Errorf("Describe cannot be run with empty <backup_id> or <timestamp>")
	}
	var code int
	var text string
	var err error
	if isValidTimestamp(backupID) {
		body := map[string]interface{}{"ts": backupID}
		code, text, err = bc.doRequest("GET", bc.host+"/find", body)
	} else {
		code, text, err = bc.doRequest("GET", bc.host+"/listbackups/"+backupID, nil)
	}
	if err != nil {
		return "", err
	}
	if code > 400 {
		bc.log("Error executing request, Response: Status-Code: %d, Content: %s", code, text)
		return "", fmt.Errorf("%s", text)
	}
	return text, nil
}

func (bc *backupClient) getStatus(jobID string) (string, error) {
	if jobID == "" {
		return "", fmt.Errorf("Status cannot be run with empty <backup_id> or <timestamp>")
	}
	code, text, err := bc.doRequest("GET", bc.host+"/jobstatus/"+jobID, nil)
	if err != nil {
		return "", err
	}
	if code > 400 {
		bc.log("Error executing request, Response: Status-Code: %d, Content: %s", code, text)
		return "", fmt.Errorf("%s", text)
	}
	return text, nil
}

func isValidTimestamp(s string) bool {
	n, err := strconv.ParseInt(s, 10, 64)
	if err != nil {
		return false
	}
	return n >= 0
}

func parseCliArgs(args []string) (*cliArgs, error) {
	p := &cliArgs{
		timeout:    60,
		properties: make(map[string]string),
	}

	i := 0
	var positionals []string

	for i < len(args) {
		arg := args[i]
		switch {
		case arg == "-h" || arg == "--help":
			p.help = true
		case arg == "-u" || arg == "--username":
			i++
			if i >= len(args) {
				return nil, fmt.Errorf("expected value for %s", arg)
			}
			p.username = args[i]
		case arg == "-p" || arg == "--password":
			i++
			if i >= len(args) {
				return nil, fmt.Errorf("expected value for %s", arg)
			}
			p.password = args[i]
		case arg == "--host":
			i++
			if i >= len(args) {
				return nil, fmt.Errorf("expected value for %s", arg)
			}
			p.host = args[i]
		case arg == "--verify":
			i++
			if i >= len(args) {
				return nil, fmt.Errorf("expected value for %s", arg)
			}
			p.verify = args[i]
		case arg == "-i" || arg == "--incremental":
			p.incremental = true
		case arg == "-v" || arg == "--verbose":
			p.verbose = true
		case arg == "-w" || arg == "--wait":
			p.wait = true
		case arg == "-t" || arg == "--timeout":
			i++
			if i >= len(args) {
				return nil, fmt.Errorf("expected value for %s", arg)
			}
			t, err := strconv.Atoi(args[i])
			if err != nil {
				return nil, fmt.Errorf("invalid timeout value: %s", args[i])
			}
			p.timeout = t
		case arg == "--dbs":
			i++
			for i < len(args) && !strings.HasPrefix(args[i], "-") {
				p.dbs = append(p.dbs, args[i])
				i++
			}
			continue
		case arg == "--properties":
			i++
			for i < len(args) && !strings.HasPrefix(args[i], "-") && strings.Contains(args[i], "=") {
				parts := strings.SplitN(args[i], "=", 2)
				p.properties[parts[0]] = parts[1]
				i++
			}
			continue
		case arg == "--input" || arg == "--in":
			i++
			if i >= len(args) {
				return nil, fmt.Errorf("expected value for %s", arg)
			}
			p.input = args[i]
		case arg == "--output" || arg == "--out":
			i++
			if i >= len(args) {
				return nil, fmt.Errorf("expected value for %s", arg)
			}
			p.output = args[i]
		default:
			if strings.HasPrefix(arg, "-") {
				return nil, fmt.Errorf("unrecognized argument: %s", arg)
			}
			positionals = append(positionals, arg)
		}
		i++
	}

	if len(positionals) > 0 {
		p.command = positionals[0]
	}
	if len(positionals) > 1 {
		p.positional = positionals[1]
	}

	return p, nil
}

func printUsage() {
	fmt.Fprintf(os.Stderr, `usage: %[1]s [-h]
                {backup,b,restore,r,list,l,describe,get,d,status,s,evict,e}
                ...

Backup Daemon CLI is a shell client for Backup Daemon REST API.

optional arguments:
  -h, --help            show this help message and exit

commands:
  {backup,b,restore,r,list,l,describe,get,d,status,s,evict,e}
    backup (b)          Perform backup. Returns <backup_id>.
    restore (r)         Perform restore. Must be used with backup identifier
                        `+"`restore <backup_id>`"+` or `+"`restore <timestamp>`"+`. Returns <job_id>.
    list (l)            List backups
    describe (get, d)   Describe backup. Must be used with backup identifier
                        `+"`describe <backup_id>`"+` or `+"`describe <timestamp>`"+`.
    status (s)          Describe status of operation. Must be used with job
                        identifier `+"`status <job_id>`"+`
    evict (e)           Evict backup. Can be used with backup identifier
                        `+"`evict <backup_id>`"+`. If used without parameters all
                        evictable backups will be removed.

Examples:

%[1]s backup
%[1]s restore 20230303T101010
%[1]s restore 1692321321312 --dbs database1 database2 --properties cluster=aws mode=all --wait
%[1]s describe 20230303T101010
%[1]s status 66a0f51b-e6ac-4e89-b2c7-48774ed05a7c
`, programName)
}

func printCommonOptions() {
	fmt.Fprintf(os.Stderr, `
optional arguments:
  -h, --help            show this help message and exit
  --username USERNAME, -u USERNAME
                        The username of Backup Daemon API. By default the
                        value of 'BACKUP_DAEMON_API_CREDENTIALS_USERNAME'
                        environment variable is used.
  --password PASSWORD, -p PASSWORD
                        The password of Backup Daemon API. By default the
                        value of 'BACKUP_DAEMON_API_CREDENTIALS_PASSWORD'
                        environment variable is used.
  --host HOST           The url address of Backup Daemon REST API. By default
                        the local address is used `+"`http(s)://localhost:8080(8443)`"+`.
  --verify VERIFY       The path to CA certificate to verify HTTPS connection
                        or `+"`false`"+` to disable it. By default the value of
                        `+"`{CERTS_PATH}/ca.crt`"+` is used.
  --incremental, -i     Use incremental API for execution commands. Default false.
  --verbose, -v         Verbose output of executing commands. By default it
                        responses with final output of Backup Daemon API only.
  --wait, -w            Wait for command execution for async commands like
                        `+"`backup`"+` or `+"`restore`"+`. By default all commands are
                        asynchronous.
  --timeout TIMEOUT, -t TIMEOUT
                        Timeout for commands execution (in seconds). By
                        default: 60.
  --dbs DBS [DBS ...]   Databases to perform operation delimited by space. If
                        not specified - all databases are used.
  --properties PROPERTIES [PROPERTIES ...]
                        Additional properties as key=value delimited by space.
  --input INPUT, --in INPUT
                        Specify the path to the file with input data for
                        command.
  --output OUTPUT, --out OUTPUT
                        Specify the path to the file with output data of
                        command.
`)
}

func printCommandHelp(cmd string) {
	switch cmd {
	case "backup", "b":
		fmt.Fprintf(os.Stderr, "usage: %s backup [-h] [options]\n\nPerform backup. Returns <backup_id>.\n", programName)
		printCommonOptions()
		fmt.Fprintf(os.Stderr, `Examples:

%[1]s backup
%[1]s backup --dbs database1 database2 --properties cluster=aws mode=all --wait
%[1]s backup --out /backup-storage/latest-id
`, programName)
	case "restore", "r":
		fmt.Fprintf(os.Stderr, "usage: %s restore [-h] [backup_id] [options]\n\nPerform restore. Must be used with backup identifier\n`restore <backup_id>` or `restore <timestamp>`. Returns <job_id>.\n", programName)
		printCommonOptions()
		fmt.Fprintf(os.Stderr, `Examples:

%[1]s restore 20230303T101010
%[1]s restore 1692321321312
%[1]s restore --in /backup-storage/latest-id --dbs database1 database2 --properties cluster=aws mode=all --wait
`, programName)
	case "list", "l":
		fmt.Fprintf(os.Stderr, "usage: %s list [-h] [options]\n\nList backups.\n", programName)
		printCommonOptions()
	case "describe", "get", "d":
		fmt.Fprintf(os.Stderr, "usage: %s describe [-h] [backup_id] [options]\n\nDescribe backup. Must be used with backup identifier\n`describe <backup_id>` or `describe <timestamp>`.\n", programName)
		printCommonOptions()
	case "status", "s":
		fmt.Fprintf(os.Stderr, "usage: %s status [-h] [job_id] [options]\n\nDescribe status of operation. Must be used with job identifier `status <job_id>`.\n", programName)
		printCommonOptions()
	case "evict", "e":
		fmt.Fprintf(os.Stderr, "usage: %s evict [-h] [backup_id] [options]\n\nEvict backup. Can be used with backup identifier `evict <backup_id>`.\nIf used without parameters all evictable backups will be removed.\n", programName)
		printCommonOptions()
	default:
		printUsage()
	}
}

func main() {
	parsed, err := parseCliArgs(os.Args[1:])
	if err != nil {
		fmt.Fprintf(os.Stderr, "error: %v\n", err)
		printUsage()
		os.Exit(2)
	}

	if parsed.help {
		if parsed.command != "" {
			printCommandHelp(parsed.command)
		} else {
			printUsage()
		}
		return
	}

	if parsed.command == "" {
		printUsage()
		return
	}

	client := newBackupClient(
		parsed.host, parsed.username, parsed.password, parsed.verify,
		parsed.dbs, parsed.properties,
		parsed.wait, parsed.verbose, parsed.timeout, parsed.incremental,
	)

	var output string

	switch parsed.command {
	case "backup", "b":
		output, err = client.performBackup()
	case "restore", "r":
		backupID := parsed.positional
		if parsed.input != "" {
			data, readErr := os.ReadFile(parsed.input)
			if readErr != nil {
				fmt.Fprintf(os.Stderr, "%v\n", readErr)
				os.Exit(1)
			}
			backupID = string(data)
		}
		output, err = client.performRestore(backupID)
	case "list", "l":
		output, err = client.getBackupList()
	case "describe", "get", "d":
		backupID := parsed.positional
		if parsed.input != "" {
			data, readErr := os.ReadFile(parsed.input)
			if readErr != nil {
				fmt.Fprintf(os.Stderr, "%v\n", readErr)
				os.Exit(1)
			}
			backupID = string(data)
		}
		output, err = client.describeBackup(backupID)
	case "status", "s":
		jobID := parsed.positional
		if parsed.input != "" {
			data, readErr := os.ReadFile(parsed.input)
			if readErr != nil {
				fmt.Fprintf(os.Stderr, "%v\n", readErr)
				os.Exit(1)
			}
			jobID = string(data)
		}
		output, err = client.getStatus(jobID)
	case "evict", "e":
		backupID := parsed.positional
		if parsed.input != "" {
			data, readErr := os.ReadFile(parsed.input)
			if readErr != nil {
				fmt.Fprintf(os.Stderr, "%v\n", readErr)
				os.Exit(1)
			}
			backupID = string(data)
		}
		output, err = client.performEvict(backupID)
	default:
		printUsage()
		return
	}

	if err != nil {
		fmt.Fprintf(os.Stderr, "%v\n", err)
		os.Exit(1)
	}

	fmt.Println(output)

	if parsed.output != "" {
		if writeErr := os.WriteFile(parsed.output, []byte(output), 0644); writeErr != nil {
			fmt.Fprintf(os.Stderr, "error writing output file: %v\n", writeErr)
			os.Exit(1)
		}
	}
}
