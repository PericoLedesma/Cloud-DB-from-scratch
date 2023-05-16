import shelve

# Open the shelf file
with shelve.open('storage.db') as shelf:
    # Print all key-value pairs in the shelf
    print("All key-value pairs:")
    counter = 1
    for key, value in shelf.items():
        print(f"Item {counter}==> {key} | {value}")
        counter += 1