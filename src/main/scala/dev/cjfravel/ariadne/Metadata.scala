package dev.cjfravel.ariadne

import com.google.gson.annotations.SerializedName
import java.util

case class Metadata(
    @SerializedName("format") var format: String,
    @SerializedName("schema") var schema: String,
    @SerializedName("indexes") var indexes: util.List[String]
)