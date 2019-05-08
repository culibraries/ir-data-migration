## IR Bepress Metadata

This repository is a docker container that use input from Bepress metadata to parse and download all data.


### Installation

* docker build -t dm_ir .

### Run

1. cp env_file_example to env_file
2. set AWS and Cybercom tokens

3. docker run -d dm_ir 

        If you you want to use your own data file
        
        $ docker run -d -v <myfile>:/app/<myfile> dm_ir python generateJson.py <myfile> 

4. Add Download Stats

        $ docker run -d -v <myfile>:/app/<myfile> dm_ir python setStats.py <myfile>
