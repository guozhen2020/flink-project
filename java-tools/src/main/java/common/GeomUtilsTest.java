package common;

import java.util.ArrayList;
import java.util.List;

public class GeomUtilsTest {
    public static void main(String[] args) {
        double px = Math.random();
        double py = Math.random();
        double length = 100;
        double[][] squarePoints={{px*1000,py*1000},{px*1000+length,py*1000+length}};
        System.out.println(String.format("square: %s,%s  %s,%s",squarePoints[0][0],squarePoints[0][1],squarePoints[1][0],squarePoints[1][1]));

        Long start = System.currentTimeMillis();
        List<double[]> pointList = new ArrayList<>();
        for(int x=0;x<1000;x++){
            for(int y=0;y<1000;y++){
                double[] point=new double[]{x,y};
                if(IsInside(point,squarePoints)){
                    pointList.add(point);
                }
            }

        }
        Long end = System.currentTimeMillis();
        System.out.println(pointList.size());
        System.out.println("耗时："+(end-start));
    }

    private static boolean IsInside(double[] point,double[][] squarePoints){
        return point[0] < squarePoints[1][0] && point[0] >= squarePoints[0][0] &&
                point[1] < squarePoints[1][1] && point[1] >= squarePoints[0][1];
    }
}
