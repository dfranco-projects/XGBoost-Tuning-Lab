# XGBoost-Tuning-Lab (WIP)

Welcome to the **XGBoost Tuning Lab** repository!  

This project is dedicated to develop a reproducible framework for **Extreme Gradient Boosting** (XGBoost) exploring and iteratively optimizing its hyperparameters, showcasing different techinques. XGBoost is a model developed by Tianqi Chen (Chen & Guestrin, 2016) and inspired by Friedman’s (2001) Gradient Boosting, introducing improvements in speed through parallelization while providing less tendency to overfit with new parameters such as regularization terms.

The focus is on exploring and optimizing hyperparameters to maximize model performance for a **churn prediction** task using the **Tesco Dataset**.

**Remember that you should always look to understand your data**, this repository not only demonstrates how to optimize your XGBoost but also takes you through the thought process and decision-making when working with your data.

Always remember that the most important step is to understand your data! 

You can find the dataset [here](https://www.kaggle.com/datasets/blastchar/telco-customer-churn/code?datasetId=13996&sortBy=voteCount).
And XGBoost's documentation [here](https://xgboost.readthedocs.io/en/stable/)

---

## Key Objectives  

- Understand and explore the data.
- Develop a reproducible pipeline for **XGBoost hyperparameter tuning**.  
- Explore different tuning methods such as **Grid Search**, **Random Search**, and **Bayesian Optimization**.
- Analyze the impact of hyperparameters on model performance and interpretability.  
- Apply the optimized model to churn prediction and comparing it with the baseline model.  

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
