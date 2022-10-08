Batch Processing for Banking Systems

Team:
1. Movinuddin
2. Sanjana Gutta
3. Revanth Kumar

Things to install: Jupyter Notebook, we need to pip install pyspark.

CCFraudDetectionML.ipynb:
This file has the code for the creating a ML model used for predicting if a credit card transaction is fraud or not.

Dataset Link: https://www.kaggle.com/datasets/mlg-ulb/creditcardfraud

Steps followed to create the model:

1. Initially we initialize the spark session.
2. Read all the data from the dataset into a pyspark dataframe.
3. Convert the pyspark dataframe to pandas dataframe for ease of use.
4. As the dataset is highly imbalanced with the number of fraud and non-fraud records, we try to reduce the non-fraud record according with the fraud records to get a good balance for creating an effective model.
5. After all the manipulations to the dataset are done, we convert the pandas dataframe back to the pyspark dataframe.
6. Next, we create the training and testing datasets with 80-20 ratio.
7. We are using RandomForestClassifier to build the model as this is a classification problem.
8. Once the model is created, we use the test data to test the accuracy of the model.
