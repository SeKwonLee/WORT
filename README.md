## WORT: Write Optimal Radix Tree for Persistent Memory Storage Systems (FAST 2017)
WORT is PM-aware radix tree. In our paper, we show that the radix tree can be more appropriate 
as an efficient PM indexing structure. This is because the radix tree structure is determined 
by the prefix of the inserted keys and also does not require tree rebalancing operations and 
node granularity updates. This repository provides the implementation of WORT and WOART that were
used for the studies of our paper.

```
@inproceedings {201600,
author = {Se Kwon Lee and K. Hyun Lim and Hyunsub Song and Beomseok Nam and Sam H. Noh},
title = {{WORT}: Write Optimal Radix Tree for Persistent Memory Storage Systems},
booktitle = {15th {USENIX} Conference on File and Storage Technologies ({FAST} 17)},
year = {2017},
isbn = {978-1-931971-36-2},
address = {Santa Clara, CA},
pages = {257--270},
url = {https://www.usenix.org/conference/fast17/technical-sessions/presentation/lee-se-kwon},
publisher = {{USENIX} Association},
month = feb,
}
```

### Note
This implementation is based on "[Adaptive Radix Trees implemented in C](https://github.com/armon/libart)"
