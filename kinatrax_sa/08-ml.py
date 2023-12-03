# Databricks notebook source
# MAGIC %md
# MAGIC #### Problem Statement:
# MAGIC Given batters' biomechanic motion sequences, can we segregate them into meaningful groups based purely on the similarity of their swing characteristics? Can we also visualize these to determine which groups current batters and prospective signings might fall into?
# MAGIC
# MAGIC #### Approach:
# MAGIC We use unsupervised machine learning techniques like Dimensionality Reduction through Principal Component Analysis (PCA) followed by clustering, to group motion sequences into distinct clusters based on swing similarity.
# MAGIC - **Principal Component Analysis (PCA)** is used to reduce the dimensionality of large datasets by finding the directions of maximum variance in the data and projecting the data onto a lower-dimensional subspace while preserving most of the variability in the data. Biomechanics motion captures are very high-dimensional (27 joints x 16 transformation matrix elements each), so by performing PCA on them we can reduce them to lower-dimensional vectors without significant information loss, which would further allow us to plot these sequences in lower-dimensional spaces.
# MAGIC - **K-Means Clustering** is a partitioning algorithm that groups n observations into k clusters in which each observation belongs to the cluster with the nearest mean, a centroid, serving as a prototype of the cluster. This allows us to segregate motion sequences into clusters of similar motions based on the swing motion sequences
# MAGIC
# MAGIC #### Outcomes:
# MAGIC We can use the described unsupervised machine learning process to group batter's motion sequences into distinct groups based on swing similarities. This can facilitate analysing how different batters swing against certain pitchers or pitch types, and get insights into what works well and what doesn't. Furthermore, when scouting and signing new players or looking for stratetic replacements for injured players, a clustering algorithm like this could be the first step towards determining how good a fit the player could potentially be based solely on their swing characteristics.

# COMMAND ----------

# Import the required libraries. Databricks ML Runtimes provide fully loaded and configured ML environments.
import numpy as np
import pandas as pd
import mlflow
import matplotlib.pyplot as plt
from pyspark.sql.functions import col
from pyspark.ml.feature import VectorAssembler
from sklearn.cluster import KMeans
from sklearn.metrics import silhouette_score
from sklearn.preprocessing import StandardScaler
from sklearn.decomposition import PCA
import seaborn as sns
sns.set()

# Set global variables
USERNAME = 'hussain.vahanvaty@databricks.com' # Set your own username

# COMMAND ----------

# MAGIC %md
# MAGIC #### Data Preprocessing

# COMMAND ----------

# Read in the motion_sequence_batting_key_frames silver table
# Use the automatic, distributed dataset profiling to generate summary statistics of the dataset
df_msbkf = spark.read.table('kinatrax.silver.batting_motion_sequence_batting_key_frames')
display(df_msbkf)

# COMMAND ----------

# Selecting one Key Frame per motion sequence record (this could be expanded to include several)
df_msbkf = df_msbkf.where(col('KeyFrameNames') == 'BallContact')

# Select the required motion sequence columns
cols_to_select = [col for col in df_msbkf.columns if ('_R1' in col or '_R2' in col or '_R3' in col or '_t' in col) and 'Center' not in col and 'Top' not in col and 'Knob' not in col]

df = df_msbkf.select([col(c) for c in cols_to_select])

# Convert the DataFrame to a Pandas DataFrame
pandas_df = df.toPandas()

# COMMAND ----------

# Standardize all motion sequence features (transformation matrix elements for each joint) to normalize for different scales of measurement. This ensures equal contribution of each feature when computing clustering distances
scaler = StandardScaler()
scaled_features = scaler.fit_transform(pandas_df)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Clustering Model Training

# COMMAND ----------

# Create a new MLflow experiment
mlflow.set_experiment(f"/Users/{USERNAME}/kinatrax_pca_kmeans")

# COMMAND ----------

# Setting the number of principal components to 3 for easy visualizatio
pca = PCA(n_components=3)
pca.fit(scaled_features)

# COMMAND ----------

# Transform the scaled features into principal component vectors
scores_pca = pca.transform(scaled_features)

# COMMAND ----------

# We’ll incorporate the newly obtained PCA scores in the K-means algorithm. That's how we can perform clustering based on principal components scores instead of the original features.

# K-means clustering with PCA
# We fit K means using the treansformed data from the PCA
# We use Mlflow tracking to track experiment parameters and metrics

wcss = []
for i in range(1,21):
  with mlflow.start_run(run_name=f'run_k={i}'):
    mlflow.log_param("k", i)
    kmeans_pca = KMeans(n_clusters=i, init='k-means++', random_state=42)
    kmeans_pca.fit(scores_pca)
    mlflow.sklearn.log_model(kmeans_pca, f"kmeans_pca_{i}")
    mlflow.log_metric("wcss", kmeans_pca.inertia_)
    wcss.append(kmeans_pca.inertia_)

# COMMAND ----------

# The next step is to plot the Within Cluster Sum of Squares (WCSS) against the number of components on a graph
plt.figure(figsize=(10,8))
plt.plot(range(1,21), wcss, marker='o', linestyle='--')
plt.xlabel('Number of Clusters')
plt.ylabel('WCSS')
plt.title('K-means with PCA Clustering')
plt.show()

# COMMAND ----------

mlflow.end_run()
# We have chosen 4 clusters using the elbow method from the graph above
# We run K-Means with n_clusters=4
with mlflow.start_run():
  kmeans_pca = KMeans(n_clusters=4, init='k-means++', random_state=42)
  # We fit our dat with the k-means pca model
  kmeans_pca.fit(scores_pca)
  mlflow.sklearn.log_model(kmeans_pca, f"kmeans_best_k={4}")

# COMMAND ----------

# We create a new dataframe with the original features and add the PCA scores and assigned clusters
df_motion_pca_kmeans = pd.concat([pandas_df.reset_index(drop=True), pd.DataFrame(scores_pca)], axis=1)
df_motion_pca_kmeans.columns.values[-3:] = ['LatentVector1', 'LatentVector2', 'LatentVector3']
# The last column we add contains the pca k-means clustering labels
df_motion_pca_kmeans['Cluster K-means PCA'] = kmeans_pca.labels_
df_motion_pca_kmeans['Cluster'] = df_motion_pca_kmeans['Cluster K-means PCA'].map({0:'Cluster1', 1:'Cluster2', 2:'Cluster3', 3:'Cluster4'})

# COMMAND ----------

display(df_motion_pca_kmeans)

# COMMAND ----------

# Plot the data by PCA components. The Y axis is the first component, the X-axis is the second
x_axis = df_motion_pca_kmeans['LatentVector2']
y_axis = df_motion_pca_kmeans['LatentVector1']
plt.figure(figsize=(10,8))
sns.scatterplot(x=x_axis, y=y_axis, hue=df_motion_pca_kmeans['Cluster'], palette=['g', 'r', 'c', 'm'])
plt.title('Batter Swing Motion Clusters by Latent Vectors')
plt.show()

# COMMAND ----------


