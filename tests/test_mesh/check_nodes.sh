function check_logs {
    for (( N=$1; N <= $2; N++ ))
    do
        echo ""
        echo "$N"
        tail -f tmp/output_$N.log | grep "Applied block" -m 1
        tail -f tmp/output_$N.log | grep "Applied mesh" -m 1
    done
}

check_logs 1 5
check_logs 11 15