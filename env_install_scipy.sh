# Tell Numpy installer where to find lapack
export LDFLAGS="-L/opt/homebrew/opt/lapack/lib"
export CPPFLAGS="-I/opt/homebrew/opt/lapack/include"
export PKG_CONFIG_PATH="/opt/homebrew/opt/lapack/lib/pkgconfig"

# See https://github.com/scipy/scipy/issues/12935
export CFLAGS=-Wno-error=implicit-function-declaration


# The location may vary - use find command to find this on your local /opt/homebrew/opt
export LAPACK=/opt/homebrew/opt/lapack/lib/liblapack.dylib
export BLAS=/opt/homebrew/opt/openblas/lib/libopenblasp-r0.3.20.dylib