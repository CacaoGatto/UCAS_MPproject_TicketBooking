package ticketingsystem;

import java.util.Objects;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicIntegerArray;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicLongArray;

public class TicketingDS implements TicketingSystem {

    /* Key parameters */

    int route_num;
    int coach_num;
    int seat_num;
    int station_num;
    int thread_num;

    /* Invalid intervals stored in bitmap format */

    static AtomicIntegerArray[] seat_record;
    int record_size;

    /* Structures to store information of sold tickets */

    static AtomicLong tid = new AtomicLong(0);
    volatile String[] sold_name;
    static AtomicIntegerArray sold_hash;

    long[][] thread_tid;

    /* Get a number of TIDs for a thread every time to avoid competition */

    private long getIdLocal () {

        /* Group by the last 7 bits (128 groups in all) */

        int thread_id = (int)(Thread.currentThread().getId()) & 0x7f;

        /* Allocate 256 more TID if the former have been used up */

        if ((thread_tid[thread_id][0] & 0x7f) == 0)
            thread_tid[thread_id][0] = tid.getAndAdd(128);

        return thread_tid[thread_id][0]++;
    }

    /* Get a compressed record of sold ticket to reduce memory usage */

    private int getHash(Ticket t) {
        int index = (t.coach - 1) * seat_num + t.seat - 1;
        return t.arrival | t.departure << 6 | t.route << 12 | index << 18;
    }

    /* Generate a new Ticket object */

    private Ticket newTicket(String passenger, int route, int index, int departure, int arrival) {
        long id = getIdLocal();
        int coach = index / seat_num + 1;
        int seat = index % seat_num + 1;
        Ticket t = new Ticket();
        t.tid = id;
        t.passenger = passenger;
        t.route = route;
        t.coach = coach;
        t.seat = seat;
        t.departure = departure;
        t.arrival = arrival;
        sold_hash.set((int) id, getHash(t));
        sold_name[(int) id] = passenger;
        return t;
    }

    /* Get the interval in bitmap format */

    private int interval(int src, int dst) {
        int src_bit = 0xffffffff << (src - 1);
        int dst_bit = 0xffffffff >>> (33 - dst);
        return src_bit & dst_bit;
    }

    /* Initialize necessary structures */

    private void initDS() {
        if (station_num > Integer.SIZE)
            throw new IllegalArgumentException();
        record_size = coach_num * seat_num;
        seat_record = new AtomicIntegerArray[route_num];
        for (int i = 0; i < route_num; i++) {
            int length = record_size;
            seat_record[i] = new AtomicIntegerArray(length);
            for (int j = 0; j < length; j++)
                seat_record[i].set(j, 0);
        }
        int max_tid = thread_num * 120000;
        sold_hash = new AtomicIntegerArray(max_tid);
        sold_name = new String[max_tid];
        for (int i = 0; i < max_tid; i++) {
            sold_name[i] = null;
            sold_hash.set(i, 0xffffffff);
        }

        thread_tid = new long[128][8];
        for (int i = 0; i < 128; i++) {
            thread_tid[i][0] = 0;
        }
    }

    /* Default Constructor */

    public TicketingDS() {
        this.route_num = 5;
        this.coach_num = 8;
        this.seat_num = 100;
        this.station_num = 10;
        this.thread_num = 16;
        initDS();
    }

    /* Constructor with input parameters */

    public TicketingDS(int route_num, int coach_num, int seat_num, int station_num, int thread_num) {
        this.route_num = route_num;
        this.coach_num = coach_num;
        this.seat_num = seat_num;
        this.station_num = station_num;
        this.thread_num = thread_num;
        initDS();
    }

    /* Interface of Buying A Ticket */

    public Ticket buyTicket(String passenger, int route, int departure, int arrival) {

        int interval = interval(departure, arrival);
        AtomicIntegerArray route_record = seat_record[route - 1];

        /* Select a random start to avoid competition */

        int end = (passenger.hashCode() & 0x7fffffff) % record_size;
        int index = end;

        do {
            /* Keep searching until succeed */
            while (true) {
                int record = route_record.get(index);
                /* Give up current seat */
                if ((record & interval) != 0) break;
                int new_record = record | interval;
                boolean res = route_record.compareAndSet(index, record, new_record);
                /* Another thread may try writing the same seat. Check again if CAS failed */
                if (res)
                    return newTicket(passenger, route, index, departure, arrival);
            }

            index++;
            if (index == record_size) index = 0;

        } while (index != end);

        return null;
    }

    /* Interface of Inquiry Remaining Tickets */

    public int inquiry(int route, int departure, int arrival) {

        int cnt = 0;
        int interval = interval(departure, arrival);
        AtomicIntegerArray route_record = seat_record[route - 1];

        /* Traverse all seats to count available seats */

        for (int i = 0; i < record_size; i++) {
            int record = route_record.get(i);
            if ((record & interval) == 0) cnt++;
        }

        return cnt;
    }

    /* Interface of Refunding A Ticket */

    public boolean refundTicket(Ticket ticket) {

        int id = (int) ticket.tid;

        /* Refunding an unsold ticket will fail */

        if (sold_name[id] == null) return false;

        /* Refunding a ticket with wrong passenger name will fail */

        if (!Objects.equals(ticket.passenger, sold_name[id]))
            return false;

        /* Refunding a ticket with wrong information (except name) will fail */

        int t_hash = getHash(ticket);
        if (!sold_hash.compareAndSet(id, t_hash, 0xffffffff))
            return false;

        int interval = ~interval(ticket.departure, ticket.arrival);
        int index = (ticket.coach - 1) * seat_num + ticket.seat - 1;
        AtomicIntegerArray record = seat_record[ticket.route - 1];

        /* Reset the sold bitmap */

        while (true) {
            int old_record = record.get(index);
            int new_record = old_record & interval;
            if (record.compareAndSet(index, old_record, new_record)) break;
        }

        return true;
    }

    /* Unrealized */

    public boolean buyTicketReplay(Ticket ticket) {
        return true;
    }

    public boolean refundTicketReplay(Ticket ticket) {
        return true;
    }

}
