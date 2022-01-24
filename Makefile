.PHONY: all
all:
	@echo see README.md to get started

.PHONY: nwc
nwc:
	./nwc-submit.sh --sampling-fraction 0.0004 --molecules-per-ml-task 16384 --ml-prefetch 1 --ml-excess-queue 4

.PHONY: xtb
xtb:
	./xtb-submit.sh

.PHONY: clean
clean:
	rm -rf __pycache__ redis.out runs scratch
