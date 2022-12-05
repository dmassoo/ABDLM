from minio import Minio
from minio.error import S3Error

# demo for minio
def main():

    # todo create and add your keys
    client = Minio(
        "localhost:9000",
        access_key="Qmb3toLFm8Ev3nPp",
        secret_key="TU4S2gnDJkyZUWMM6TDl8gQmtbDEVJR2",
        secure=False
    )

    # Make 'asiatrip' bucket if not exist.
    found = client.bucket_exists("abdlm")
    if not found:
        client.make_bucket("abdlm")
    else:
        print("Bucket 'abdlm' already exists")

    # Upload '/home/user/Photos/asiaphotos.zip' as object name
    # 'asiaphotos-2015.zip' to bucket 'asiatrip'.
    client.fput_object(
        "abdlm", "example.ork", "example.orc",
    )
    print(f"File is loaded successfully")


if __name__ == "__main__":
    try:
        main()
    except S3Error as exc:
        print("error occurred.", exc)