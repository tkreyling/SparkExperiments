# Step-by-Step installation guide for a LAN Mesos cluster
For your SparExperiments and SparkScalaExperiments we setup a Mesos cluster where we could submit jobs to. 
The cluster contained one master and three agents.
As hardware base we used three MacBook Pro (MBP) with 8 GB / 16 GB RAM and 8 CPU Cores each.

One MBP ran the Mesos master and one agent; the other two MBP one agent each.

The release version of Mesos was: 1.0.1

We decided to compile the Mesos binaries from source because we found no adequate pre-build binaries for our scenario.

# Obstacles
To get the setup running we followed the installation 'Getting started' guide from Mesos project page: 
http://mesos.apache.org/gettingstarted/

At a later point we realized that the user account and rights setup for a cluster is a bit of a hassle. 
That's because mesos tries to run the jobs with the same user on every agent.
As a workaround to avoid the setup of ACL lists we created a spare user 'mesos' with admin privileges on each MBP to run the master and agent processes.

Another Obstacle was the locale settings during the compile and test: On one MBP the locale setting for LANG='de_DE.UTF-8' differed from the rest of the parameters 'en_US.UTF-8'.
This broke a test which was already reported as flaky. Setting all local parameters to 'en_US.UTF-8' solved this problem.

# Setup
(Redo these steps on every computer of your cluster)

## User setup
* Create a user 'mesos' with admin privileges. We did that in the OSX settings panel.
* Switch to that user in OSX (or in all your shell session)
* Check your locale settings (see the obstacles section above); for the newly created user they should be empty; which is ok

## Comply with Mesos system requirements
(The described setup of hostnames in ```/etc/hosts``` in the original guide was not necessary because hostname broadcast and resolution was already setup in our network.)
```
# Install Command Line Tools.
$ xcode-select --install

## Install Homebrew.
$ ruby -e "$(curl -fsSL https://raw.githubusercontent.com/Homebrew/install/master/install)"

# Install Java. Even if you have one ;-)
$ brew install Caskroom/cask/java

# Install libraries.
$ brew install wget git autoconf automake libtool subversion maven 
```
## Setup the working directories
The Mesos master and agent need a working directory each. The standard location is in ```/var/lib```. Make sure that the directory is accessible for the user ```mesos```.
```
# Setup working dir for Mesos master (on master only) (may need 'sudo')
$ mkdir /var/lib/mesos
$ chown mesos:admin /var/lib/mesos
$ chmod a+r+w /var/lib/mesos

# Setup working dir for Mesos agent (may need 'sudo')
$ mkdir /var/lib/mesos-agent
$ chown mesos:admin /var/lib/mesos-agent
$ chmod a+r+w /var/lib/mesos-agent
```

## Install Mesos
```
# Download the mesos package from 
http://mesos.apache.org/downloads/

# Extract the file
$ tar -zxf <download_file_.tar.gz> 

# Copy the resulting folder to your preferred install location (may need 'sudo')
$ cp -r mesos-1.0.1 /Applications/

* Go into the folder and create a build directory 
$ cd /Applications/mesos-1.0.1/
$ mkdir build 

# Change in to the build directory
$ cd build

# Configure the build
$ ../configure

# Make the build. Don't forget to use all your CPU cores via -j <number of cores> parameter
$ make -j 8 

# Wait a while ;-)

# Run the test suite
$ make -j 8 check

# Wait once more ...

# The pyhton test suite failed for us; but we didn't used it at all -> so it's ok ;-)

# If the java test framework fails here, you may have a locale issue (see obstacles)

# Install the build and tested Mesos executables and libs to your computer
# Maybe you need some sudo priviliges as prefix
$ make - j 8 install

# Finsihed: Yeaaah! Go to run Mesos instructions
```