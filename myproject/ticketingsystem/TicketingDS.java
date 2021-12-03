package ticketingsystem;

import java.util.Objects;
import java.util.concurrent.atomic.AtomicIntegerArray;
import java.util.concurrent.atomic.AtomicLong;

public class TicketingDS implements TicketingSystem {

    int route_num;
    int coach_num;
    int seat_num;
    int station_num;
    int thread_num;

    /* Memory Padding for Better Cache Access */
    int padding = 0;

    static AtomicIntegerArray[] seat_record;
    int record_size;

    static AtomicLong tid = new AtomicLong(0);
    volatile String[] sold_name;
    static AtomicIntegerArray sold_hash;

    long[] thread_tid;

    private int getHash(Ticket t) {
        int index = (t.coach - 1) * seat_num + t.seat - 1;
        return t.arrival | t.departure << 6 | t.route << 12 | index << 18;
    }

    private long getId() {
        int thread_id = (int)(Thread.currentThread().getId()) & 0x3f;
        if ((thread_tid[thread_id] & 0xff) == 0)
            thread_tid[thread_id] = tid.getAndAdd(256);
        return thread_tid[thread_id]++;
    }

    private Ticket newTicket(String passenger, int route, int index, int departure, int arrival) {
        long id = getId();
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

    private int interval(int src, int dst) {
        int src_bit = 0xffffffff << (src - 1);
        int dst_bit = 0xffffffff >>> (33 - dst);
        return src_bit & dst_bit;
    }

    private void initDS() {
        if (station_num > Integer.SIZE)
            throw new IllegalArgumentException();
        record_size = coach_num * seat_num;
        seat_record = new AtomicIntegerArray[route_num];
        for (int i = 0; i < route_num; i++) {
            seat_record[i] = new AtomicIntegerArray(record_size << padding);
            for (int j = 0; j < record_size << padding; j++)
                seat_record[i].set(j, 0);
        }
        thread_tid = new long[64];
        for (int i = 0; i < 64; i++) {
            thread_tid[i] = 0;
        }
        int max_tid = thread_num * 120000;
        sold_hash = new AtomicIntegerArray(max_tid);
        sold_name = new String[max_tid];
        for (int i = 0; i < max_tid; i++) {
            sold_name[i] = null;
            sold_hash.set(i, 0xffffffff);
        }
    }

    public TicketingDS() {
        this.route_num = 5;
        this.coach_num = 8;
        this.seat_num = 100;
        this.station_num = 10;
        this.thread_num = 16;
        initDS();
    }

    public TicketingDS(int route_num, int coach_num, int seat_num, int station_num, int thread_num) {
        this.route_num = route_num;
        this.coach_num = coach_num;
        this.seat_num = seat_num;
        this.station_num = station_num;
        this.thread_num = thread_num;
        initDS();
    }

    public Ticket buyTicket(String passenger, int route, int departure, int arrival) {
        int interval = interval(departure, arrival);
        AtomicIntegerArray route_record = seat_record[route - 1];
        int end = (passenger.hashCode() & 0x7fffffff) % record_size; //random.nextInt(record_size);
        int index = end;
        do {
            while (true) {
                int record = route_record.get(index << padding);
                if ((record & interval) != 0) break;
                int new_record = record | interval;
                boolean res = route_record.compareAndSet(index << padding, record, new_record);
                if (res)
                    return newTicket(passenger, route, index, departure, arrival);
            }
            index++;
            if (index == record_size) index = 0;
        } while (index != end);
        return null;
    }

    public int inquiry(int route, int departure, int arrival) {
        int cnt = 0;
        int interval = interval(departure, arrival);
        for (int i = 0; i < record_size; i++) {
            int record = seat_record[route - 1].get(i << padding);
            if ((record & interval) == 0) cnt++;
        }
        return cnt;
    }

    public boolean refundTicket(Ticket ticket) {
        int id = (int) ticket.tid;
        if (sold_name[id] == null) return false;
        if (!Objects.equals(ticket.passenger, sold_name[id]))
            return false;
        int t_hash = getHash(ticket);
        if (!sold_hash.compareAndSet(id, t_hash, 0xffffffff))
            return false;
        int interval = ~interval(ticket.departure, ticket.arrival);
        int index = (ticket.coach - 1) * seat_num + ticket.seat - 1;
        AtomicIntegerArray record = seat_record[ticket.route - 1];
        boolean res = true;
        while (res) {
            int old_record = record.get(index << padding);
            int new_record = old_record & interval;
            res = !record.compareAndSet(index << padding, old_record, new_record);
        }
        return true;
    }

    public boolean buyTicketReplay(Ticket ticket) {
        return true;
    }

    public boolean refundTicketReplay(Ticket ticket) {
        return true;
    }

}
