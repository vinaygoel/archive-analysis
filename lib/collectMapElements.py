@outputSchema("values:bag{t:tuple(key, value)}")
def collectMapElements(map_dict):
    return map_dict.items()
