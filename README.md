# XGBoost-Tuning-Lab (WIP)

Welcome to the __XGBoost Tuning Lab__ repository!

This project is dedicated to developing a reproducible framework for __Extreme Gradient Boosting (XGBoost)__, exploring and iteratively optimizing its hyperparameters while showcasing different techniques. XGBoost, developed by Tianqi Chen (Chen & Guestrin, 2016), is inspired by Friedman’s (2001) Gradient Boosting. It introduces improvements in speed through parallelization and reduces the tendency to overfit by incorporating regularization terms.

The focus of this project is on exploring and optimizing hyperparameters to maximize model performance for a __churn prediction__ task using the __Tesco Dataset__.

__Remember that understanding your data is always the first step__. This repository not only demonstrates how to optimize your XGBoost model but also guides you through the thought process and decision-making when working with your data.

You can find... <br>
The dataset [here](https://www.kaggle.com/datasets/blastchar/telco-customer-churn/code?datasetId=13996&sortBy=voteCount)<br>
XGBoost's documentation [here](https://xgboost.readthedocs.io/en/stable/)

---

## Key Objectives  

- Understand and explore the data.
- Develop a reproducible pipeline for __XGBoost hyperparameter tuning__.
- Explore different tuning methods such as __Grid Search__, __Random Search__, and __Bayesian Optimization__.
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
