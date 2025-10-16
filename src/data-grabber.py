from pcpartpicker import API
import subprocess
import json


def grab_cpu_data():
    p = API()
    cpus = p.retrieve("cpu")

    print(f"Type of cpus: {type(cpus)}")
    cpus = cpus.to_json()
    print(f"Type of cpus after to_json: {type(cpus)}")

    # Split the string object into a dictionary
    print("Converting JSON string to dictionary...")
    cpus = json.loads(cpus)
    cpus = json.loads(cpus['cpu'])
    print(f"Type of cpus after json.loads: {type(cpus)}")

    print(f"First CPU entry: {cpus[0]}")

    return cpus

if __name__ == "__main__":
    grab_cpu_data()