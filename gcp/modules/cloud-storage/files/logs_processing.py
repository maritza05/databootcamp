import sys
import argparse
import xml.etree.ElementTree as ET

from pyspark.context import SparkContext
from pyspark.sql.session import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType


logs_info_schema = StructType(
    [
        StructField("log_date", StringType(), True),
        StructField("device", StringType(), True),
        StructField("os", StringType(), True),
        StructField("location", StringType(), True),
        StructField("ip", StringType(), True),
        StructField("phone_number", StringType(), True),
    ]
)


def _select_text(doc, xpath):
    nodes = [e.text for e in doc.findall(xpath) if isinstance(e, ET.Element)]
    return next(iter(nodes), None)


def _extract_values_from_xml(payload):
    doc = ET.fromstring(payload)
    return {
        "log_date": _select_text(doc, "log/logDate"),
        "device": _select_text(doc, "log/device"),
        "os": _select_text(doc, "log/os"),
        "location": _select_text(doc, "log/location"),
        "ip": _select_text(doc, "log/ipAddress"),
        "phone_number": _select_text(doc, "log/phoneNumber"),
    }


def _parse_args(argv):
    parser = argparse.ArgumentParser()
    parser.add_argument("--input_file", required=True)
    parser.add_argument("--output_path", required=True)
    return parser.parse_args()


_extract_columns_from_xml = F.udf(_extract_values_from_xml, logs_info_schema)


if __name__ == "__main__":
    flags = _parse_args(sys.argv[1:])
    sc = SparkContext()
    spark = SparkSession(sc)

    log_reviews_df = spark.read.option("header", True).csv(flags.input_file)

    clean_df = log_reviews_df.withColumn(
        "info", _extract_columns_from_xml("log")
    ).select(
        "id_review",
        F.to_date(F.col("info.log_date"), "MM-dd-yyyy").alias("log_date"),
        "info.device",
        "info.os",
        "info.location",
        "info.ip",
        "info.phone_number",
    )

    clean_df.write.format("avro").save(flags.output_path)
