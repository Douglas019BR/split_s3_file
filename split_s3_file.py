import ipdb
import time
import boto3
import io

from nexus_whatsapp_commons.aws import AWSS3Service


def split_and_upload_to_s3_by_lines(
    bucket, file_key, split_bucket, filename, extension, lines_per_chunk
):
    s3 = AWSS3Service()
    ipdb.set_trace()

    # Leitura em streaming do arquivo do S3
    response = s3.get_object(Bucket=bucket, Key=file_key)
    content_stream = response["Body"]

    # Processamento em streaming e envio para o S3
    current_chunk_lines = []
    current_chunk_index = 0

    for line in content_stream.iter_lines():
        line = line.decode("utf-8")
        current_chunk_lines.append(line)

        if len(current_chunk_lines) >= lines_per_chunk:
            # Enviar parte para o S3
            part_content = "\n".join(current_chunk_lines).encode("utf-8")
            s3.put_object(
                Bucket=split_bucket,
                Key=f"{filename}_{current_chunk_index}.{extension}",
                Body=io.BytesIO(part_content),
            )

            # Limpar lista para a próxima parte
            current_chunk_lines = []
            current_chunk_index += 1

    # Enviar a última parte se houver linhas restantes
    if current_chunk_lines:
        part_content = "\n".join(current_chunk_lines).encode("utf-8")
        s3.put_object(
            Bucket=split_bucket,
            Key=f"{filename}_{current_chunk_index}.{extension}",
            Body=io.BytesIO(part_content),
        )


start_time = time.time()
# Parâmetros
bucket = "original-bucket"
file_key = "x/xxx/contatos.csv"
split_bucket = "split_bucket"
filename = "contatos"
extension = "csv"
lines_per_chunk = 10000  # número de linhas por parte

# Chamada da função
split_and_upload_to_s3_by_lines(
    bucket, file_key, split_bucket, filename, extension, lines_per_chunk
)
elapsed_time = time.time() - start_time
print(elapsed_time)
