package kriging;

/**
 * Created by mata on 7/7/15.
 */
public class GeoPoint {
    double x;
    double y;
    double z;

    public void set_x(double val_x){
        x = val_x;
    }

    public void set_y(double val_y){
        y = val_y;
    }

    public void set_z(double val_z){
        z = val_z;
    }

    public double get_x(){
        return x;
    }

    public double get_y(){
        return y;
    }

    public double get_z(){
        return z;
    }
};
