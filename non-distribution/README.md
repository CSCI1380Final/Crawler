# non-distribution

This milestone aims (among others) to refresh (and confirm) everyone's
background on developing systems in the languages and libraries used in this
course.

By the end of this assignment you will be familiar with the basics of
JavaScript, shell scripting, stream processing, Docker containers, deployment
to AWS, and performance characterization—all of which will be useful for the
rest of the project.

Your task is to implement a simple search engine that crawls a set of web
pages, indexes them, and allows users to query the index. All the components
will run on a single machine.

## Getting Started

To get started with this milestone, run `npm install` inside this folder. To
execute the (initially unimplemented) crawler run `./engine.sh`. Use
`./query.js` to query the produced index. To run tests, do `npm run test`.
Initially, these will fail.

### Overview

The code inside `non-distribution` is organized as follows:

```
.
├── c            # The components of your search engine
├── d            # Data files like the index and the crawled pages
├── s            # Utility scripts for linting and submitting your solutions
├── t            # Tests for your search engine
├── README.md    # This file
├── crawl.sh     # The crawler
├── index.sh     # The indexer
├── engine.sh    # The orchestrator script that runs the crawler and the indexer
├── package.json # The npm package file that holds information like JavaScript dependencies
└── query.js     # The script you can use to query the produced global index
```

### Submitting

To submit your solution, run `./scripts/submit.sh` from the root of the stencil. This will create a
`submission.zip` file which you can upload to the autograder.

# M0: Setup & Centralized Computing

> Add your contact information below and in `package.json`.

* name: Zejun Zhou

* email: zejun_zhou@brown.edu

* cslogin: zzhou190


## Summary

> Summarize your implementation, including the most challenging aspects; remember to update the `report` section of the `package.json` file with the total number of hours it took you to complete M0 (`hours`), the total number of JavaScript lines you added, including tests (`jsloc`), the total number of shell lines you added, including for deployment and testing (`sloc`).


My implementation consists of 8 components addressing T1--8. The most challenging aspect was implementing merge.js because the initial structure of global and local are different and require data processing.


## Correctness & Performance Characterization


> Describe how you characterized the correctness and performance of your implementation.


To characterize correctness, we developed 8 student tests that test the following cases: stem words, convert HTML content to text, find and extract the URLs, processing the text including remove the stop words, inverted index that mapping sorted terms to URLs, merging coming indices with global indices. 

The implementation first including completing stem.js, which use the natural library to stem the input words. Following that, I have complete getText.js and getURLs to convert HTML content to text and find and extract the URLs. After that, I complete the process.sh to convert each line to one word per line and filtering such as excluding the stop words. I complete merge.js to merge the local index with the global index. After that, I complete query.js, which provide a query interface. 


*Performance*: The throughput of various subsystems is described in the `"throughput"` portion of package.json. The characteristics of my development machines are summarized in the `"dev"` portion of package.json.


## Wild Guess

> How many lines of code do you think it will take to build the fully distributed, scalable version of your search engine? Add that number to the `"dloc"` portion of package.json, and justify your answer below.

I believe that at least 3000 lines of code are needed for thte scalable search engine. Excepting the logics, there are more works required such as building the system to balance the traffics for scalability. Also, the implementation are required to build the recovery system under the distributed version. For example, we want to include the logics to pass message between different of nodes. At this time, I do not know exactly how many more lines of code are required. My guess is about 3000 lines of code since this assignment takes about 300 lines of code including testing. 