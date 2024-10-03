#!bash
echo '{ "disabledExtensions": { "@jupyter/collaboration-extension": true }, "lockedExtensions": { "@jupyter/collaboration-extension": true } }' > /home/cdsw/.jupyter/labconfig/page_config.json
pip install pandas numpy scikit-learn seaborn matplotlib xgboost imblearn lightgbm 
# hit a version mismatch with our pkl file
pip install cloudpickle==3.0.0 psutil==5.9.5 colorama==0.4.6 distributed==2024.5.0
pip install flask sqlalchemy
mkdir -p data
