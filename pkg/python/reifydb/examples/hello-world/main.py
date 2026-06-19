# SPDX-License-Identifier: Apache-2.0
# Copyright (c) 2026 ReifyDB

from reifydb import Embedded
from tabulate import tabulate

db = Embedded()
r = db.tx('map 1, 1 + 4')

print(tabulate(r[0]['rows'], headers=r[0]['headers']))