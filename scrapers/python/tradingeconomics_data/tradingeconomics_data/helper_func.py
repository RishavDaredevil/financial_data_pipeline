import base64
import zlib


def decode_obfuscated_data(base64_string: str, key: str) -> str | None:
    """
    Decodes, deobfuscates (XOR), and decompresses a Base64-encoded string
    using a given key. Assumes data is GZIP-compressed after XOR obfuscation.

    Parameters:
        base64_string (str): The encoded string from the network.
        key (str): The obfuscation key used for XOR.

    Returns:
        str | None: The final decoded UTF-8 string or None if failed.
    """
    try:
        # Step 1: Base64 decode
        decoded_bytes = base64.b64decode(base64_string)

        # Step 2: XOR deobfuscation
        key_bytes = key.encode()
        key_len = len(key_bytes)
        xor_bytes = bytearray(decoded_bytes)
        for i in range(len(xor_bytes)):
            xor_bytes[i] ^= key_bytes[i % key_len]

        # Step 3: GZIP decompression (zlib with gzip flag)
        decompressed_data = zlib.decompress(xor_bytes, zlib.MAX_WBITS | 32)

        # Step 4: Decode to string
        return decompressed_data.decode('utf-8')

    except Exception as e:
        print("Decoding failed:", e)
        return None
