# es-index-java
Build elasticsearch index for covid-19 corpus (cosi-132a final project), data of this project are from Kaggle's COVID-19 Open Research Dataset Challenge (CORD-19)
## Getting Started
Below are prequisites and instructions for using this project
### Prerequisites
Java version 1.8.0
### Instrucitons
1. Download project as zip file or clone this repository
2. Download all dependencies in the pom.xml file by importing the pom.xml file(Intellij IDEA, etc) or install maven and run command 
```
mvn install
```

3. Download all data sets from kaggle website: https://www.kaggle.com/allen-institute-for-ai/CORD-19-research-challenge
4. Put all those data files in a folder called data in your project's root directory
5. In the same root directory, create two folders index-data and processed
6. Run Parser.main() to preprocess data in all json files
7. Run MetadataCSVParser.main() to process the metadata.csv file
8. Run IndexJsonFile.main() to start building es index. (Make sure to start the elasticsearch instance on your local environment)

## To be continued...
