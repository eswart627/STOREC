# block size 64 MB
BLOCK_SIZE = 64 * 1024 * 1024      
K = 4                          
M = 2                               

DATA_PER_STRIPE = K * BLOCK_SIZE
BLOCKS_PER_STRIPE = K + M