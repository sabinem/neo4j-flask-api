PAGE_SIZE = 20

def get_pagination(page=1):
    if not page:
        skip = 0
    else:
        skip = page * PAGE_SIZE - PAGE_SIZE
    return skip
