@outputSchema("line:chararray")
def TupleToString(*record):
	items = []
	for item in record :
		if item is not None:
			items.append(item.tostring())
	return ':'.join(items);

def CountElement(*record):
	return len(record)
