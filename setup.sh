#!bash
echo '{ "disabledExtensions": { "@jupyter/collaboration-extension": true }, "lockedExtensions": { "@jupyter/collaboration-extension": true } }' > /home/cdsw/.jupyter/labconfig/page_config.json
pip install pandas numpy scikit-learn seaborn matplotlib xgboost imblearn lightgbm 
mkdir -p data
