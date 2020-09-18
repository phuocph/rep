package main

import (
	"bytes"
	"flag"
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

func readConfig(configFile string) *Config {
	raw, err := ioutil.ReadFile(configFile)
	if err != nil {
		panic(err)
	}

	config := &Config{}
	err = yaml.Unmarshal(raw, &config)
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

func buildRestoreCommand(dbConfig db, fileName string) string {
	// options := "--no-privileges --no-owner --blobs --format=custom --verbose"
	options := "-x -O -c --if-exists "
	cmd := fmt.Sprintf(
		"PGPASSWORD=%s pg_restore -h %s -p %d -U %s -d %s %s %s",
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

func main() {
	var configFile string
	flag.StringVar(&configFile, "f", "config.yml", "env mode")
	flag.Parse()
	fmt.Println("-> Config file: ", configFile)

	config := readConfig(configFile)
	step := 1
	fmt.Printf("%d. Checking config...\n", step)
	checkingConfig(config)

	step++
	fmt.Printf("%d. SSH to %s\n", step, config.Server.Host)
	client := Dial(config.Server)
	defer client.Close()

	suffix := fmt.Sprintf("%d", int(time.Now().UnixNano()))
	dumpFile := fmt.Sprintf("/tmp/%s_%s.dump", config.Server.DB.Database, suffix)

	dumpCmd := buildDumpCommand(config.Server.DB, dumpFile)
	step++
	fmt.Printf("%d. Dumping database %s in %s\n", step, config.Server.DB.Database, config.Server.Host)
	runRemoteCmd(client, dumpCmd)
	defer func() {
		step++
		fmt.Printf("%d. Remove temp dump file %s in %s\n", step, dumpFile, config.Server.Host)
		runRemoteCmd(client, fmt.Sprintf("rm -f %s", dumpFile))
	}()

	step++
	fmt.Printf("%d. Copy dump file %s to local\n", step, dumpFile)
	copiedDumpFile := copyDumpFile(config.Server, dumpFile)
	defer func() {
		step++
		fmt.Printf("%d. Remove local temp copied file %s\n", step, copiedDumpFile)
		runLocalCmd(fmt.Sprintf("rm -f %s", copiedDumpFile))
	}()

	step++
	fmt.Printf("%d. Restoring %s to databae %s\n", step, copiedDumpFile, config.LocalDB.Database)
	restoreCmd := buildRestoreCommand(config.LocalDB, copiedDumpFile)
	runLocalCmd(restoreCmd)
}
