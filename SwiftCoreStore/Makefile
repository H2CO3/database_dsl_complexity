# The Swift Package Manager does NOT work
# without Xcode on macOS. The Command Line Tools
# are simply not enough, it wants the `xctest`
# tool, and the PackageDescription framework is
# not available in the command-line installation,
# either.
#
# Without dwelling on how *utterly* ridicuous
# this is, I instead just opted for driving the
# Swift compiler from the command line by a plain
# old Makefile.
#
# In order to have at least *some* structure to
# the project anyway, any eventual dependencies
# are added as Git submodules.

SWIFTC = xcrun -sdk macosx swiftc -j 8 -O -whole-module-optimization

CORE_STORE_SRC = $(wildcard Dependencies/CoreStore/Sources/*.swift)
CORE_STORE_LIB = Build/CoreStore.framework/CoreStore
CORE_STORE_MOD = Build/CoreStore.framework/Modules/CoreStore.swiftmodule

DRIVER_SRC = $(wildcard Sources/*.swift)
DRIVER_EXE = Build/driver

all: $(DRIVER_EXE)

run: $(DRIVER_EXE)
	Build/driver

clean:
	rm -rf $(DRIVER_EXE) Build/CoreStore.framework 

$(CORE_STORE_MOD):
	mkdir -p $(CORE_STORE_MOD)

$(CORE_STORE_LIB): $(CORE_STORE_MOD) $(CORE_STORE_SRC)
	$(SWIFTC) \
		-emit-library -emit-module \
		-module-name CoreStore \
		-emit-module-path $(CORE_STORE_MOD)/x86_64.swiftmodule \
		-o $@ \
		$(CORE_STORE_SRC)

$(DRIVER_EXE): $(CORE_STORE_LIB) $(DRIVER_SRC)
	$(SWIFTC) \
		-emit-executable \
		-F Build \
		-framework CoreStore \
		-o $@ \
		$(DRIVER_SRC)

