spark-submit \
--master local[*] \
--deploy-mode client \
--py-files etl/utils/spark_utils.py \
"$1"