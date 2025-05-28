package main

import (
	"bufio"
	"flag"
	"fmt"
	"log"
	"os"
	"strconv"
	"strings"

	"gfs/pkg/client"
)

var (
	masterAddr = flag.String("master", "localhost:8000", "Master server address")
)

// Command handler function type
type commandFunc func(args []string) error

// Map of commands to handler functions
var commands = map[string]struct {
	handler commandFunc
	usage   string
}{
	"ls":     {handleList, "ls <path> - List directory contents"},
	"mkdir":  {handleMkdir, "mkdir <path> - Create a directory"},
	"create": {handleCreate, "create <path> - Create a new file"},
	"write":  {handleWrite, "write <path> <offset> <data> - Write data to a file"},
	"read":   {handleRead, "read <path> <offset> <length> - Read data from a file"},
	"rm":     {handleRemove, "rm <path> - Remove a file or directory"},
	"help":   {handleHelp, "help - Show this help message"},
	"exit":   {handleExit, "exit - Exit the client"},
}

// Global client
var gfsClient *client.Client

func main() {
	// Parse command line flags
	flag.Parse()
	
	// Create client
	var err error
	gfsClient, err = client.NewClient(client.Config{
		MasterAddress: *masterAddr,
	})
	if err != nil {
		log.Fatalf("Failed to create client: %v", err)
	}
	
	// Interactive mode
	fmt.Println("GFS Client - Type 'help' for commands")
	
	scanner := bufio.NewScanner(os.Stdin)
	for {
		fmt.Print("gfs> ")
		if !scanner.Scan() {
			break
		}
		
		input := scanner.Text()
		if input == "" {
			continue
		}
		
		args := strings.Fields(input)
		cmd := args[0]
		
		if command, ok := commands[cmd]; ok {
			if err := command.handler(args[1:]); err != nil {
				fmt.Printf("Error: %v\n", err)
			}
		} else {
			fmt.Printf("Unknown command: %s\n", cmd)
		}
	}
}

// Command handlers

func handleHelp(args []string) error {
	fmt.Println("GFS Client Commands:")
	for _, cmd := range commands {
		fmt.Println("  " + cmd.usage)
	}
	return nil
}

func handleList(args []string) error {
	if len(args) < 1 {
		return fmt.Errorf("usage: ls <path>")
	}
	
	entries, err := gfsClient.ListDirectory(args[0])
	if err != nil {
		return err
	}
	
	for _, entry := range entries {
		fmt.Println(entry)
	}
	return nil
}

func handleMkdir(args []string) error {
	if len(args) < 1 {
		return fmt.Errorf("usage: mkdir <path>")
	}
	return gfsClient.CreateDirectory(args[0])
}

func handleCreate(args []string) error {
	if len(args) < 1 {
		return fmt.Errorf("usage: create <path>")
	}
	
	_, err := gfsClient.Create(args[0])
	return err
}

func handleWrite(args []string) error {
	if len(args) < 3 {
		return fmt.Errorf("usage: write <path> <offset> <data>")
	}
	
	offset, err := strconv.Atoi(args[1])
	if err != nil {
		return fmt.Errorf("invalid offset: %w", err)
	}
	
	file, err := gfsClient.Open(args[0])
	if err != nil {
		return err
	}
	defer file.Close()
	
	return file.Write(offset, []byte(strings.Join(args[2:], " ")))
}

func handleRead(args []string) error {
	if len(args) < 3 {
		return fmt.Errorf("usage: read <path> <offset> <length>")
	}
	
	offset, err := strconv.Atoi(args[1])
	if err != nil {
		return fmt.Errorf("invalid offset: %w", err)
	}
	
	length, err := strconv.Atoi(args[2])
	if err != nil {
		return fmt.Errorf("invalid length: %w", err)
	}
	
	file, err := gfsClient.Open(args[0])
	if err != nil {
		return err
	}
	defer file.Close()
	
	data, err := file.Read(offset, length)
	if err != nil {
		return err
	}
	
	fmt.Printf("%s\n", string(data))
	return nil
}

func handleRemove(args []string) error {
	if len(args) < 1 {
		return fmt.Errorf("usage: rm <path>")
	}
	return gfsClient.Delete(args[0])
}

func handleExit(args []string) error {
	fmt.Println("Exiting...")
	os.Exit(0)
	return nil
}