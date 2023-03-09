# odd-counter-app

Odd counter application does aggregation on odd numbers for input csv and tsv files.


## Architecture

Odd counter application is spark application written on Scala.
Main class is `Processor` that does input/output and processing on data:
- reads mixed csv and tsv files from provided input path, transforms to typed dataset (`Record` class)
- processes records and provides aggregated dataset of odd numbers count
- saves aggregated dataset of odd numbers to provided output path 


Key point in this application is reading mixed csv and tsv files.
Though it could be solved in different ways, the approach taken in this application is the following:
- reads input data as text where each file is a separate row in dataframe
- removes header as first line of the file
- determines the file type either csv ot tsv by separator sign
- does parse row to the columns and casts to required int values
- returns list of typed `Record` objects from each file

This approach allows to read each input file exactly once - determine the separator and parse the file in single IO.


## Usage

To compile, test and assembly project:
`sbt clean package assembly`

To run application
`odd-counter-app <input_path> <output_path> <aws_profile>`


## Notes

1. Application could be enhanced with AWS integration when proper access keys provided.

2. Implementation algorithms:

a) The solution provided in this application allows to read each file once only.

Complexity should be O(N^2/E), where E number of executors.\
Considering that cost of reading is N, and cost of processing is N as well. 

b) Another approach to solve this task, would be:
- split csv/tsv files, by reading its header and first line
- create 2 separate list of files - csv, tsv
- read each set of files separately using spark internals
- combine 2 datasets in single using by union

Complexity should be O(2*N^2/E), where E number of executors.

c) Preferred approach - communicate with upstream team and discuss a possibility to write csv and tsv files in separate folders:
- read csv / tsv files separately using spark internals
- combine 2 datasets in single using by union

Complexity should be O(N^2/E), where E number of executors.\
This implementation has 2 benefits:
- proper and clean storage of files
- clean and concise processing algorithm using spark internals
