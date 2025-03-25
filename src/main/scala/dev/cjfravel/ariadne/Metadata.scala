package dev.cjfravel.ariadne

import com.google.gson.annotations.SerializedName
import java.util

case class Metadata(
    @SerializedName("format") var format: String,
    @SerializedName("schema") var schema: String,
    @SerializedName("files") var files: util.List[FileMetadata],
    @SerializedName("indexes") var indexes: util.List[String]
)

case class FileMetadata(
    @SerializedName("file") var file: String,
    @SerializedName("timestamp") var timestamp: Long,
    @SerializedName("indexed") var indexed: Boolean
)