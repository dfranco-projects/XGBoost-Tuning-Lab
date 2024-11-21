# XGBoost-Tuning-Lab (WIP)

Welcome to the **XGBoost Tuning Lab** repository!

This project is dedicated to developing a reproducible framework for **Extreme Gradient Boosting (XGBoost)**, exploring and iteratively optimizing its hyperparameters while showcasing different techniques. XGBoost, developed by Tianqi Chen (Chen & Guestrin, 2016), is inspired by Friedman’s (2001) Gradient Boosting. It introduces improvements in speed through parallelization and reduces the tendency to overfit by incorporating regularization terms.

The focus of this project is on exploring and optimizing hyperparameters to maximize model performance for a **churn prediction** task using the **Tesco Dataset**.

**Remember that understanding your data is always the first step**. This repository not only demonstrates how to optimize your XGBoost model but also guides you through the thought process and decision-making when working with your data.

You can find... <br>
The dataset [here](https://www.kaggle.com/datasets/blastchar/telco-customer-churn/code?datasetId=13996&sortBy=voteCount){:target="_blank"} <br>
XGBoost's documentation [here](https://xgboost.readthedocs.io/en/stable/){:target="_blank"}

---

## Key Objectives  

- Understand and explore the data.
- Develop a reproducible pipeline for **XGBoost hyperparameter tuning**.
- Explore different tuning methods such as **Grid Search**, **Random Search**, and **Bayesian Optimization**.
- Analyze the impact of hyperparameters on model performance and interpretability.
- Apply the optimized model to churn prediction and compare it with the baseline model.

---

## Getting Started  

### Prerequisites  

- Python 3.8 or higher  
- Key libraries: `xgboost`, `pandas`, `numpy`, `scikit-learn`, `optuna`, `matplotlib`, `seaborn`

### Installation  

1. Clone the repository:  
   ```bash
      git clone https://github.com/dfranco-projects/XGBoost-Tuning-Lab
      cd XGBoost-Tuning-Lab
   ```
