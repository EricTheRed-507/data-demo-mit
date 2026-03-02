The following is what I did to get my system up and running with the mockdata-daemon and my implementation file ingesting topics.

1. I used the local kafka configuration, with:
   ./gradlew kafka1ComposeUp

Note: As the documentation for the data-demo says, there are multiple ways to do this, but this is the one that I got working after one of the other methods didn't work for me.

2. Start a the consumer application. For my topoplogy, mine is InStateStreamersVsEventCapacity.java. I just created a copy of the SampleJavaKafkaConsumer.java and changed the input topics and print statements to get a baseline.

Note: I just use VSCode and press the "Run Java" button on the file, but you could launch this from a separate terminal.

3. Run the mockdata-daemon from another terminal:
   ./gradlew bootRunDaemon -Pspring.profiles.active=kafka
   
Now you should be ingesting data into your consumer that's being generated from the mockdata-daemon.

My example subscribes to the 4 topics that I need for my topology, so you'll each need to configure yours to subscibe to the ones you need. I'd suggest creating a copy of my file and changing it to be named for your topologies that you're working on, change the name, and put it in the same "phase3" folder that this file and my starting example are in. But you two might be more well versed in Java and have a better suggestion for how we want to structure this repository.

