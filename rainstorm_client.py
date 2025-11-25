# rainstorm_client.py
import argparse
import sys

VALID_OPS = {"transform", "filter", "aggregate"}

def parse_operator(op_exe, op_args_list):
    if op_exe not in VALID_OPS:
        print(f"Error: invalid operator '{op_exe}'. Must be one of: transform, filter, aggregate.")
        sys.exit(1)

    if op_exe == "aggregate":
        if len(op_args_list) != 0:
            print("Error: 'aggregate' takes no arguments.")
            sys.exit(1)
        return {"exe": op_exe, "args": None}

    if op_exe == "filter":
        if len(op_args_list) != 1:
            print("Error: 'filter' requires exactly one argument: \"pattern\"")
            sys.exit(1)
        return {"exe": op_exe, "args": op_args_list[0]}

    if op_exe == "transform":
        if len(op_args_list) != 1:
            print('Error: "transform" requires one argument: "old new"')
            sys.exit(1)

        parts = op_args_list[0].split()
        if len(parts) != 2:
            print('Error: transform args must be exactly two words inside one string: "old new"')
            sys.exit(1)

        old, new = parts
        return {"exe": op_exe, "args": (old, new)}

    raise RuntimeError("Unhandled operator case.")

def main():
    parser = argparse.ArgumentParser(description="RainStorm client")

    parser.add_argument("Nstages", type=int, choices=[1, 2])
    parser.add_argument("Ntasks_per_stage", type=int, choices=[1, 2, 3])
    parser.add_argument("rest", nargs=argparse.REMAINDER)

    args = parser.parse_args()

    Nstages = args.Nstages
    Ntasks = args.Ntasks_per_stage
    rest = args.rest

    # -----------------------------
    # Stage 1 operator
    # -----------------------------
    if len(rest) == 0:
        print("Error: missing operator for stage 1.")
        sys.exit(1)

    op1_exe = rest[0]
    rest = rest[1:]

    op1_args_list = []
    if op1_exe != "aggregate":
        if len(rest) == 0:
            print(f"Error: missing argument for operator {op1_exe}.")
            sys.exit(1)
        op1_args_list.append(rest[0])
        rest = rest[1:]

    op1 = parse_operator(op1_exe, op1_args_list)

    # -----------------------------
    # Stage 2 operator (if needed)
    # -----------------------------
    op2 = None
    if Nstages == 2:

        if len(rest) == 0:
            print("Error: missing operator for stage 2.")
            sys.exit(1)

        op2_exe = rest[0]
        rest = rest[1:]

        op2_args_list = []
        if op2_exe != "aggregate":
            if len(rest) == 0:
                print(f"Error: missing argument for operator {op2_exe}.")
                sys.exit(1)
            op2_args_list.append(rest[0])
            rest = rest[1:]

        op2 = parse_operator(op2_exe, op2_args_list)

    # -----------------------------
    # Now parse <hydfs_src_directory> <hydfs_dest_filename>
    # -----------------------------
    if len(rest) < 2:
        print("Error: missing <hydfs_src_directory> and/or <hydfs_dest_filename>.")
        sys.exit(1)

    hydfs_src_directory = rest[0]
    hydfs_dest_filename = rest[1]
    rest = rest[2:]

    # -----------------------------
    # Parse exactly_once, autoscale_enabled
    # -----------------------------
    if len(rest) < 2:
        print("Error: missing <exactly_once> and/or <autoscale_enabled>.")
        sys.exit(1)

    exactly_once = rest[0].lower()
    autoscale_enabled = rest[1].lower()
    rest = rest[2:]

    if exactly_once not in ("true", "false"):
        print("Error: exactly_once must be 'true' or 'false'.")
        sys.exit(1)

    if autoscale_enabled not in ("true", "false"):
        print("Error: autoscale_enabled must be 'true' or 'false'.")
        sys.exit(1)

    exactly_once = (exactly_once == "true")
    autoscale_enabled = (autoscale_enabled == "true")

    if exactly_once and autoscale_enabled:
        print("Error: exactly_once and autoscale_enabled cannot both be true.")
        sys.exit(1)

    # -----------------------------
    # Parse input_rate, LW, HW
    # -----------------------------

    if len(rest) < 1:
        print("Error: missing <input_rate>.")
        sys.exit(1)

    # input_rate is always required
    try:
        input_rate = int(rest[0])
        if input_rate <= 0:
            raise ValueError
    except ValueError:
        print("Error: input_rate must be a positive integer.")
        sys.exit(1)

    rest = rest[1:]

    # Autoscale enabled → LW and HW required
    if autoscale_enabled:
        if len(rest) < 2:
            print("Error: LW and HW are required when autoscale_enabled is true.")
            sys.exit(1)

        try:
            LW = int(rest[0])
            HW = int(rest[1])
        except ValueError:
            print("Error: LW and HW must be integers.")
            sys.exit(1)

        if LW <= 0:
            print("Error: LW must be > 0.")
            sys.exit(1)

        if HW <= LW:
            print("Error: HW must be > LW.")
            sys.exit(1)

        rest = rest[2:]

    # Autoscale disabled → LW and HW must NOT be provided
    else:
        if len(rest) != 0:
            print("Error: LW and HW should not be provided when autoscale_enabled is false.")
            sys.exit(1)

        LW = None
        HW = None



    # -----------------------------
    # Error if anything remains
    # -----------------------------
    if len(rest) != 0:
        print(f"Error: extra arguments: {rest}")
        sys.exit(1)
        

    # -----------------------------
    # Print parsed values
    # -----------------------------
    print("Nstages =", Nstages)
    print("Ntasks_per_stage =", Ntasks)
    print("op1 =", op1)
    if op2 is not None:
        print("op2 =", op2)
    print("hydfs_src_directory =", hydfs_src_directory)
    print("hydfs_dest_filename =", hydfs_dest_filename)
    print("exactly once =", exactly_once)
    print("autoscale enabled =", autoscale_enabled)
    print("input rate =", input_rate)
    print("LW =", LW if LW else 0)
    print("HW =", HW if HW else 0)

if __name__ == "__main__":
    main()
