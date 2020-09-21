package main

import (
	"bytes"
	"fmt"
	"io/ioutil"
	"net"
	"os/exec"
	"time"

	"golang.org/x/crypto/ssh"
	"gopkg.in/yaml.v2"
)

type db struct {
	Host     string `yaml:"host"`
	Port     int    `yaml:"port"`
	Database string `yaml:"database"`
	Username string `yaml:"username"`
	Password string `yaml:"password"`
}

type server struct {
	Host           string `yaml:"host"`
	Port           string `yaml:"port"`
	User           string `yaml:"user"`
	PrivateKeyFile string `yaml:"private_key_file"`
	DB             db     `yaml:"db"`
}

type Config struct {
	Server  server `yaml:"server"`
	LocalDB db     `yaml:"local_db"`
}

var stringConfig = `
server:
  host: xxx
  port: xxx
  user: xxx
  private_key_file: xxx
  db:
    host: xxx
    port: xxx
    database: xxx
    username: xxx
    password: xxx

local_db:
  host: xxx
  port: xxx
  database: xxx
  username: xxx
  password: xxx

`

func readConfigFromString(s string) *Config {
	config := &Config{}
	fmt.Println(s)
	err := yaml.Unmarshal([]byte(s), &config)
	if err != nil {
		panic(err)
	}

	return config
}

func Dial(config server) *ssh.Client {
	key, err := ioutil.ReadFile(config.PrivateKeyFile)
	if err != nil {
		panic(err)
	}
	signer, err := ssh.ParsePrivateKey(key)
	if err != nil {
		panic(err)
	}

	sshClientConfig := &ssh.ClientConfig{
		User: config.User,
		Auth: []ssh.AuthMethod{
			ssh.PublicKeys(signer),
		},
		HostKeyCallback: ssh.HostKeyCallback(func(hostname string, remote net.Addr, key ssh.PublicKey) error { return nil }),
	}

	address := fmt.Sprintf("%s:%s", config.Host, config.Port)
	client, err := ssh.Dial("tcp", address, sshClientConfig)
	if err != nil {
		panic(err)
	}

	return client
}

func buildDumpCommand(dbConfig db, fileName string) string {
	// options := "--no-privileges --no-owner --blobs --format=custom --verbose"
	options := "-Fc -x"
	cmd := fmt.Sprintf(
		"PGPASSWORD=%s pg_dump -h %s -p %d -U %s -d %s %s -f %s",
		dbConfig.Password,
		dbConfig.Host,
		dbConfig.Port,
		dbConfig.Username,
		dbConfig.Database,
		options,
		fileName,
	)

	return cmd
}

func buildRestoreCommand(dbConfig db, database, fileName string) string {
	// options := "--no-privileges --no-owner --blobs --format=custom --verbose"
	options := "-x -O -c --if-exists "
	cmd := fmt.Sprintf(
		"PGPASSWORD=%s pg_restore -h %s -p %d -U %s -d %s %s %s",
		dbConfig.Password,
		dbConfig.Host,
		dbConfig.Port,
		dbConfig.Username,
		database,
		options,
		fileName,
	)

	return cmd
}

func runRemoteCmd(client *ssh.Client, cmd string) {
	session, err := client.NewSession()
	if err != nil {
		panic(err)
	}
	defer session.Close()

	var stderr bytes.Buffer
	session.Stderr = &stderr
	if err := session.Run(cmd); err != nil {
		fmt.Println(stderr.String())
		panic(err)
	}
}

func runLocalCmd(runCmd string) {
	cmd := exec.Command("bash", "-c", runCmd)
	var stderr bytes.Buffer
	cmd.Stderr = &stderr
	if err := cmd.Run(); err != nil {
		fmt.Println(stderr.String())
		panic(err)
	}
}

func copyDumpFile(serverConfig server, dumpFileName string) string {
	copiedFile := dumpFileName
	scpCmd := fmt.Sprintf(
		"scp %s@%s:%s %s",
		serverConfig.User,
		serverConfig.Host,
		dumpFileName,
		copiedFile,
	)
	runLocalCmd(scpCmd)

	return copiedFile
}

func checkingConfig(config *Config) {
	localDBExistsCmd := fmt.Sprintf(
		"PGPASSWORD=%s psql -h %s -p %d -U %s -d %s -c \"SELECT 1\"",
		config.LocalDB.Password,
		config.LocalDB.Host,
		config.LocalDB.Port,
		config.LocalDB.Username,
		config.LocalDB.Database,
	)
	runLocalCmd(localDBExistsCmd)
}

func runPSQLCmd(dbConfig db, accessForRunningDB, cmd string) {
	psqlCmd := fmt.Sprintf(
		"PGPASSWORD=%s psql -h %s -p %d -U %s -d %s",
		dbConfig.Password,
		dbConfig.Host,
		dbConfig.Port,
		dbConfig.Username,
		accessForRunningDB,
	)

	runCmd := fmt.Sprintf("%s -c \"%s\"", psqlCmd, cmd)
	runLocalCmd(runCmd)
}

func printStep(step int, s string, args ...interface{}) int {
	step++
	s = fmt.Sprintf(s, args...)
	fmt.Printf("%d. %s\n", step, s)
	return step
}

func main() {
	config := readConfigFromString(stringConfig)
	step := 0
	step = printStep(step, "Checking config...")
	checkingConfig(config)

	step = printStep(step, "SSH to %s", config.Server.Host)
	client := Dial(config.Server)
	defer client.Close()

	suffix := fmt.Sprintf("%d", int(time.Now().UnixNano()))
	dumpFile := fmt.Sprintf("/tmp/%s_%s.dump", config.Server.DB.Database, suffix)

	dumpCmd := buildDumpCommand(config.Server.DB, dumpFile)
	step = printStep(step, "Dumping database %s in %s", config.Server.DB.Database, config.Server.Host)
	runRemoteCmd(client, dumpCmd)
	defer func() {
		step = printStep(step, "Remove temp dump file %s in %s", dumpFile, config.Server.Host)
		runRemoteCmd(client, fmt.Sprintf("rm -f %s", dumpFile))
	}()

	step = printStep(step, "Copy dump file %s to local", dumpFile)
	copiedDumpFile := copyDumpFile(config.Server, dumpFile)
	defer func() {
		step = printStep(step, "Remove local temp copied file %s", copiedDumpFile)
		runLocalCmd(fmt.Sprintf("rm -f %s", copiedDumpFile))
	}()

	intermediateDB := fmt.Sprintf("tmp_%s", suffix)
	step = printStep(step, "Create local intermediate database %s", intermediateDB)
	runPSQLCmd(
		config.LocalDB,
		config.LocalDB.Database,
		fmt.Sprintf("CREATE DATABASE %s", intermediateDB),
	)
	defer func() {
		step = printStep(step, "Drop local intermediate database %s", intermediateDB)
		runPSQLCmd(
			config.LocalDB,
			config.LocalDB.Database,
			fmt.Sprintf("DROP DATABASE IF EXISTS %s", intermediateDB),
		)
	}()

	restoredDB := fmt.Sprintf("restored_%s", suffix)
	step = printStep(step, "Create local restored database %s", restoredDB)
	runPSQLCmd(
		config.LocalDB,
		intermediateDB,
		fmt.Sprintf("CREATE DATABASE %s", restoredDB),
	)
	defer func() {
		step = printStep(step, "Drop local restored database if exists %s", restoredDB)
		runPSQLCmd(
			config.LocalDB,
			config.LocalDB.Database,
			fmt.Sprintf("DROP DATABASE IF EXISTS %s", restoredDB),
		)
	}()

	step = printStep(step, "Restoring %s to databae %s", copiedDumpFile, restoredDB)
	restoreCmd := buildRestoreCommand(config.LocalDB, restoredDB, copiedDumpFile)
	runLocalCmd(restoreCmd)

	step = printStep(step, "Drop local database %s", config.LocalDB.Database)
	runPSQLCmd(
		config.LocalDB,
		intermediateDB,
		fmt.Sprintf("DROP DATABASE %s", config.LocalDB.Database),
	)

	step = printStep(step, "Rename database %s to %s", restoredDB, config.LocalDB.Database)
	runPSQLCmd(
		config.LocalDB,
		intermediateDB,
		fmt.Sprintf("ALTER DATABASE %s RENAME TO %s", restoredDB, config.LocalDB.Database),
	)
}
