# Building ETL Pipelines with Python
Create Production-Ready ETL pipelines with Python and open source Libraries. The book utilizes the Pipenv environment for dependency management and PyCharm as the recommended Integrated Development Environment (IDE).

## Tables of Contents
1. [Installation](#installation)
2. [Getting Started](#getting-started)
3. [Chapter Descriptions](#chapter-descriptions)
4. [Contributing](#contributing)
5. [License](#license)

## Installation
To set up the development environment for the Python Coding Book, follow the instructions below:

1. **Install Python**: Ensure that Python is installed on your system. You can download the latest version of Python from the official Python website.
2. **Install Pipenv**: Pipenv is used for managing dependencies. Install Pipenv by running the following command:
```shell

$ pip install pipenv

```
3. **Fork the Repository**: Fork then Clone this repository to your local machine using Git or by downloading the ZIP file from the repository's main page.
4. **Install Dependencies**: Navigate to the repository's root directory and run the following command to install the project dependencies using Pipenv:

```shell

$ pipenv install

```
5. **Set Up PyCharm (_optional_)**: If you prefer to use PyCharm as your IDE, follow the PyCharm installation instructions on the official JetBrains website.

## Getting Started
To start working with the Python Coding Book, follow the steps below:

1. **Activate the Pipenv shell**: Navigate to the repository's root directory in PyCharm and run the following command to activate the Pipenv shell:
```shell

$ pipenv shell

```
2. **Start Coding**: Follow along with this book's chapters and corresponding code examples in the repository. Each chapter is organized in its respective directory and contains code files, exercises, and supporting materials.

## Chapter Descriptions

The **_Building ETL Pipelines with Python_** consists of the following chapters:

| Index | Description |Code Files |
|---------|-------------|-----------|
| Chapter 1 | [A Primer on Python and the Development Environment]() | A brief overview of Python and setting up the development environment with an IDE and GIT.|[🔗]() |
| Chapter 2 | [Understanding Data Pipelines and the ETL Process]() | Overview of the ETL process, its significance, and the difference between ETL and ELT|[🔗]() |
| Chapter 3 | [Design Principles for ETL Pipelines]() | How to implement design patterns using open-source Python libraries for robust ETL pipelines.|[🔗]() |
| Chapter 4 | [Sourcing Insightful Data and Data Extraction Strategies]() | Strategies for obtaining high-quality data from various source systems. |[🔗]() |
| Chapter 5 | [Data Cleansing and Transformation]() | Data cleansing, handling missing data, and applying transformation techniques to achieve the desired data format.|[🔗]() |
| Chapter 6 | [Loading Transformed Data]() | Overview of best practices for data loading activities in ETL Pipelines and various data loading techniques for RDBMS and NoSQL databases. |[🔗]() |
| Chapter 7 | [Tutorial: Building a Full ETL Pipeline in Raw Python]() | Guides the creation of an end-to-end ETL pipeline using different tools and technologies, using PostGreSQL Database as an example. |[🔗]() |
| Chapter 8 | [Powerful ETL-Specific Libraries and Tools in Python]() | Creating ETL Pipelines using Python libraries: Bonobo, Odo, mETL, and Riko. Introduction to using big data tools: pETL, Luigi, and Apache Airflow. |[🔗]() |
| Chapter 9 | Primer on AWS Tools for ETL Process. | Explains AWS tools for ETL pipelines, including strategies for tool selection, creating a development environment, deployment, testing, and automation.|[🔗]() |
| Chapter 10 | Tutorial: Creating Production-Grade ETL Pipelines in AWS. | Guides the creation of ETL pipelines in AWS using step functions, Bonobo, EC2, and RDS. |[🔗]() |
| Chapter 11 | Building a Robust Deployment Pipeline in AWS. | Demonstrates using CI/CD tools to create a more resilient ETL pipeline deployment environment using: AWS CodePipeline, CodeDeploy, CodeCommit, and GIT integration.  |[🔗]() |
| Chapter 12 | Orchestration and Scaling ETL pipelines. | Covers scaling strategies, creating robust orchestration, and hands-on exercises for scaling and orchestration in ETL pipelines. |[🔗]() |
| Chapter 13 | Testing ETL Pipelines. | Examine the importance of ETL testing and strategies for catching bugs before production, including unit testing and external testing.  |[🔗]() |
| Chapter 14 | Best practices for ETL Pipelines. | Highlights industry best practices and common pitfalls to avoid when building ETL pipelines.|[🔗]() |
| Chapter 15 | Use Cases and Further Reading. | Practical exercises, mini-project outlines, and further reading suggestions are included in this chapter. Includes a case study of creating a robust ETL pipeline for New York Yellow-taxis data and US construction market data in AWS. |[🔗]() |

Each chapter directory contains code examples, exercises, and any additional resources required for that specific chapter.

## Contributing
We encourage our readers to [fork](https://docs.github.com/en/get-started/quickstart/fork-a-repo) this repository to use in tandem with each chapter. If you find any issues or have suggestions for enhancements, please feel free to submit a pull request or open an issue in the repository.

## License
The Python Coding Book is released under the Packt Publishing License. You are free to use, modify, and distribute the code and materials in this repository according to the terms of the license.

Let's get started!
