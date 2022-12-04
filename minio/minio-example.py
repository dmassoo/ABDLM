from minio import Minio
from minio.error import S3Error


def main():

    client = Minio(
        "localhost:9000",
        access_key="l6CPm5vh0J4DK1Nh",
        secret_key="ghJRtiUA66Neue45dpGArb7FEBIx3AtB",
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
    # print(
    #     "'/home/user/Photos/asiaphotos.zip' is successfully uploaded as "
    #     "object 'asiaphotos-2015.zip' to bucket 'asiatrip'."
    # )


if __name__ == "__main__":
    try:
        main()
    except S3Error as exc:
        print("error occurred.", exc)