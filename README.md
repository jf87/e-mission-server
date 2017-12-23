e-mission is a project to gather data about user travel patterns using phone
apps, and use them to provide an personalized carbon footprint, and aggregate
them to make data available to urban planners and transportation engineers.

If you are here to use ***Zephyr***, which uses the e-mission platform to
evaluate power/accuracy tradeoffs for background sensed data, please see the
[zephyr-specific README and examples](https://github.com/e-mission/e-mission-server/tree/master/zephyr).

It has two components, the backend server and the phone apps. This is the
backend server - the phone apps are available in the [e-mission-phone
repo](https://github.com/amplab/e-mission-phone)

The current build status is:
[![Build Status](https://amplab.cs.berkeley.edu/jenkins/buildStatus/icon?job=e-mission-server)](https://amplab.cs.berkeley.edu/jenkins/view/E-Mission/job/e-mission-server/)

[![Join the chat at https://gitter.im/e-mission/e-mission-server](https://badges.gitter.im/e-mission/e-mission-server.svg)](https://gitter.im/e-mission/e-mission-server?utm_source=badge&utm_medium=badge&utm_campaign=pr-badge&utm_content=badge)

The backend in turn consists of two parts - a summary of their code structure is shown below.
-![][Python_Structure]
The webapp supports a REST API, and accesses data from the database to fulfill
the queries.  A set of background scripts pull the data from external sources, and
preprocessing results ensures reasonable performance.

The installation instructions below are generally targeted towards OSX and \*nix shells such as bash. If you want to use Windows, we recomend using PowerShell (https://technet.microsoft.com/en-us/scriptcenter/dd742419), which provides similarly rich commands. If you really want to use the Command Prompt, most commands should work, but you may need to convert `/` -> `\` to make the commands work.

## Install/update: ##
-------------------

### Installation ###
is as simple as cloning the github repository.

- If you do not plan to make changes to the code, clone the master repository.

  ```
  $ git clone https://github.com/e-mission/e-mission-server.git
  ```

- If you might make changes or develop new features, fork (https://help.github.com/articles/fork-a-repo/) and clone your fork.

  ```
  $ git clone https://github.com/<username>/e-mission-server.git
  ```

### Update ###
is as simple as pulling new changes.

- If you are working off the master repository

  ```
  $ git pull origin master
  ```

- If you are working off your fork, you will need to sync your fork with the main repository (https://help.github.com/articles/syncing-a-fork/) and then pull from your fork.

  ```
  $ git pull origin master
  ```

## Dependencies: ##
-------------------

### Database: ###
1. Install [Mongodb](http://www.mongodb.org/)
  2. *Windows*: mongodb appears to be installed as a service on Windows devices and it starts automatically on reboot
  3. *OSX*: You want to install homebrew and then use homebrew to install mongodb. Follow these instruction on how to do so ---> (http://docs.mongodb.org/manual/tutorial/install-mongodb-on-os-x/)
  4. *Ubuntu*: http://docs.mongodb.org/manual/tutorial/install-mongodb-on-ubuntu/

2. Start it at the default port

     `$ mongod`

### Python distribution ###
We will use a distribution of python that is optimized for scientific
computing. The [anaconda](https://store.continuum.io/cshop/anaconda/)
distribution is available for a wide variety of platforms and includes the
python scientific computing libraries (numpy/scipy/scikit-learn) along with
native implementations for performance. Using the distribution avoids native
library inconsistencies between versions.

The distribution also includes its own version of pip, and a separate environment
management tool called 'conda'.

The distribution also includes an environment management tool called 'conda'. We will set up a separate `emission`
environment within anaconda to avoid conflicts with other applications.

- Install the anaconda distribution (https://www.anaconda.com/download). Any
  installer should be fine - setting up the `emission` environment will
  automatically choose the correct version of python. Since all required
  packages will be installed using the environment, if you are comfortable with
  the command line, you can also download the minimalist `miniconda` installer
  https://conda.io/miniconda.html

- Setup the `emission` environment.

  ```
  $ source setup/setup.sh
  ```

- Verify that you are in the right environment - your prompt should start with
  `(emission)` and the `emission` environment should be starred.

  ```
  (emission) ...$ conda env list
  # conda environments:
  #
  aws                      /..../anaconda/envs/aws
  emission              *  /..../anaconda/envs/emission
  firebase                 /..../anaconda/envs/firebase
  py27                     /..../anaconda/envs/py27
  py36                     /..../anaconda/envs/py36
  xbos                     /..../anaconda/envs/xbos
  root                     /..../anaconda
  ```

- Remember to re-run the setup script every time you pull from the main repository because the dependencies may have changed.

  ```
  $ source setup/setup.sh
  ```

- When you are done working with e-mission, you can cleanup the environment and then just delete the entire directory.

  ```
  $ source setup/teardown.sh
  $ cd ..
  $ rm -rf e-mission-server
  ```

### Python dependencies: ###

    # If you are running this in production over SSL, copy over the cherrypy-wsgiserver
    $ cp api/wsgiserver2.py <dist-packages>/cherrypy/wsgiserver/wsgiserver2.py

### Javascript dependencies ###
Run "bower install" instead if you are prompted password for 'https://github.com' after running "bower update".

    $ cd webapp
    $ bower update

## Development: ##
-------------------
In order to test out changes to the webapp, you should make the changes locally, test them and then push. Then, deployment is as simple as pulling from the repo to the real server and changing the config files slightly.

Here are the steps for doing this:

1. On OSX, start the database  (Note: mongodb appears to be installed as a service on Windows devices and it starts automatically on reboot). 

        $ mongod

1. **Optional** Copy configuration files. The files in `conf` can be used to customize the app with custom authentication options or enable external features such as place lookup and the game integration. Look at the samples in `conf/*`, copy them over and modify as necessary - e.g.

        $ find conf -name \*.sample
        # For the location -> name reverse lookup. Client will lookup if not populated.
        $ cp conf/net/ext_service/nominatim.json.sample conf/net/ext_service/nominatim.json

1. Start the server

        $ ./e-mission-py.bash emission/net/api/cfc_webapp.py

1. Test your connection to the server
  * Using a web browser, go to [http://localhost:8080](http://localhost:8080)
  * Using the iOS emulator, connect to [http://localhost:8080](http://localhost:8080)
  * Using the android emulator:
    * change `server.host` in `conf/net/api/webserver.conf` to 0.0.0.0, and 
    * connect the app to the special IP for the current host in the android emulator - [10.0.2.2](https://developer.android.com/tools/devices/emulator.html#networkaddresses)

### Loading test data ###

You may also want to load some test data.

#### Quick start ####

1. Sample timeline data used for unit testing the server code (checked in with consent of users) is at `emission/tests/data/real_examples`. You can load the data using the `load_timeline_for_day_and_user` script, like so

   ```
   $ ./e-mission-py.bash bin/debug/load_timeline_for_day_and_user.py emission/tests/data/real_examples/shankari_2015-07-22 test_july_22
   ```
   
   This will load the data as a new user with email `test_july_22` and a newly created uuid. You can run the intake pipeline for this user like so
   
   ```
   $ ./e-mission-py.bash bin/debug/intake_single_user.py -e test_july_22
   ```
   
   If you have the phone app installed, you can log in using `test_july_22` as the email, and select July 22 *2015* to see the data for that date.
   
Note that loading the data retains the object IDs. This means that if you load the same data twice with different user IDs, then only the second one will stick. In other words, if you load the file as `user1@foo.edu` and then load the same file as `user2@foo.edu`, you will only have data for `user2@foo.edu` in the database. This can be overwritten using the `--make-new` flag - e.g.

```
$ ./e-mission-py.bash bin/debug/load_timeline_for_day_and_user.py -n /tmp/data-collection-eval/results_dec_2015/ucb.sdb.android.1/timeseries/active_day_2.2015-11-27 shankari@eecs.berkeley.edu
```
   
#### Other data sources ####
1. Get your own data. You can export your timeline for a particular day via email (Profile -> Download json dump) and then load and view it as above.

1. Request access to anonymized data for research purposes by sending email to @shankari. You will be asked to consent to data retention and usage policies and will get an encrypted timeline with data from multiple users, one file per user. More information is at https://github.com/e-mission/e-mission-server/wiki/Requesting-data-as-a-collaborator
   
1. Sample timeline data from the test phones can be retrieved using the `bin/public/request_public_data.py` script. You can see the inputs to pass to the script by using
   ```
   $ bin/public/request_public_data.py --help
   ```
  1. The script loads the data into your mongodb instance using the test phone UUIDs. If you want to play with the raw data, you are good.
  2. If you want to run the existing pipeline, you need to either enable the pipeline for test phones, OR re-load the data as a normal user.

      1. To enable the pipeline for test phones, edit `emission/pipeline/scheduler.py` to add the test phones to the `TEMP_HANDLED_PUBLIC_PHONES` array.
      2. To reload the data as a normal user, save the data as a timeline file using `bin/debug/extract_timeline_for_day_and_user.py` with one of the test phone UUIDs and the time range that you downloaded the data for. This saves the data into a json file. You can then load the data using the `bin/debug/load_timeline_for_day_and_user.py` script. It requires a timeline file and a user that the timeline is being loaded as. If you wish to view this timeline in the UI after processing it, you need to login with this email.

```
            $ cd ..../e-mission-server
            $ ./e-mission-py.bash bin/debug/load_timeline_for_day_and_user.py /tmp/data-collection-eval/results_dec_2015/ucb.sdb.android.1/timeseries/active_day_2.2015-11-27 shankari@eecs.berkeley.edu
```        




### Creating fake user data ###

You may need a larger or more diverse set of data than the given test data supplies.
To create it you can run the trip generation script included in the project. 

The script works by creating random noise around starting and ending points of trips.

You can fill out options for the new data in emission/simulation/input.json. 
The different options are as follows
* radius - the number of kilometers of randomization around starting and ending points (the amount of noise)
* starting centroids - addresses you want trips to start around, as well as a weight defining the relative probability a trip will start there
* ending centroids - addresses you want trips to end around, as well as a weight defining the relative probability a trip will end there
* modes - the relative probability a user will take a trip with the given mode
* number of trips - the amount of trips the simulation should create

run the script with 
    
    $ python emission/simulation/trip_gen.py <user_name>

Because this user data is specifically designed to test our tour model creation, you can create fake tour models easily by running the `make_tour_model_from_fake_data` function in `emission/storage/decorations/tour_model_queries.py`


### Running the analysis pipeline ###

Once you have loaded the timeline, you probably want to segment it into trips and sections, smooth the sections, generate a timeline, etc. We have a unified script to do all of those, called the intake pipeline. You can run it like this.

    $ ./e-mission-py.bash bin/debug/intake_single_user.py -u <uuid>
    
You can also use the user's email id with the `-e` option. See the help message for details. Once the script is done running, places, trips, sections and stops would have been generated and stored in their respective mongodb tables, and the timelines for the last 7 days have been stored in the usercache.

We also do some modelling on the generated data. This is much more time-intensive than the intake, but also does not need to run at the same frequency as the intake pipeline. So it is pulled out to its own pipeline. If you want to work on the modelling, you need to run this pipeline as well.

    $ ./e-mission-py.bash emission/pipeline/model_stage.py
    
### Experimenting with loaded data ###

Some examples of how to retrieve and experiment with loaded/analysed data are in the `Timeseries_Sample.ipynb`

### Running unit tests ###

1. Make sure that the anaconda python is in your path

        $ which python
        /Users/shankari/OSS/anaconda/bin/python

1. Run all tests.

        $ ./runAllTests.sh

1. If you get import errors, you may need to add the current
directory to PYTHONPATH.

        $ PYTHONPATH=. ./runAllTests.sh

## Analysis ##
Several exploratory analysis scripts are checked in as ipython notebooks into
https://github.com/e-mission/e-mission-explore/. All data in the notebooks is from members of the
research team who have provided permission to use it. The results in the
notebooks cannot be replicated in the absence of the raw data, but they can be
run on data collected from your own instance as well.

The notebooks are occasionally modified and simplified as code is moved out of
them into utility functions. Original versions of the notebooks can be obtained
by looking at other notebooks with the same name, or by looking at the history
of the notebooks.

## JS Testing ##

From the webapp directory

    $ npm install karma --save-dev
    $ npm install karma-jasmine karma-chrome-launcher --save-dev

Write tests in www/js/test
If you're interested in having karma in your path and globally set, run 

    $ npm install -g karma-cli

To run tests if you have karma globally set, run 

    $ karma start my.conf.js 
    
in the webapp directory. If you didn't run the -g command, you can run
tests with 

    $ ./node_modules/karma/bin/karma start
    
in the webapp directory


## TROUBLESHOOTING: ##

1. If a python execution fails to import a module, make sure to add current
directory to your PYTHONPATH.

2. If starting the server gives a CONNECTION\_ERROR, make sure MongoDB is
actively running when you attempt to start the server.

3. After running MongoDB, if you get an error that says `dbpath does not exist` (on Windows) or `Data directory /data/db not found` (on Mac), make sure to manually create the data directory as follows.

        on Windows
        % md c:\data\db\  
or

        on Mac (the user account running mongod must have read and write permissions for the data directory)
        $ mkdir -p /data/db
        $ chmod 777 /data/db


## Design decisions: ##
----------
This site is currently designed to support travel behavior tracking and
aggregation. There is a fair amount of backend work that is more complex than
just reading and writing data from a database. So we are not using any of the
specialized web frameworks such as django or rails.

Instead, we have focused on developing the backend code and exposing it via a
simple API. I have maintained separation between the backend code and the API
glue so that we can swap out the API glue later if needed.

The API glue is currently [Bottle](http://bottlepy.org/docs/dev/index.html), which is a single file webapp framework. I
chose [Bottle](http://bottlepy.org/docs/dev/index.html) because it was simple, didn't use a lot of space, and because it
wasn't heavy weight, could easily be replaced with something more heavyweight
later.

The front-end is javascript based. In order to be consistent with the phone, it
also uses angular + ionic. javascript components are largely managed using
bower.

## Deployment: ##
----------
This is fairly complex and is under active change as we have more projects deploy their own servers with various configurations.
So I have moved it to its own wiki page:
https://github.com/e-mission/e-mission-server/wiki/Deploying-your-own-server-to-production

[Python_Structure]: https://raw.github.com/amplab/e-mission-server/master/figs/e-mission-server-module-structure.png
