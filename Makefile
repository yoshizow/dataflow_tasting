.PHONY: latenessexample latenessexample-dataflow test

latenessexample:
	mvn compile exec:java -Dexec.mainClass=com.example.LatenessExample

test:
	mvn test
