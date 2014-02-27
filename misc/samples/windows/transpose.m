% Read matrix from file
M1 = dlmread("matrix_in.txt")

% Transpose matrix
M2 = M1'

% Write transposed matrix to file 
save matrix_out.mat M2

