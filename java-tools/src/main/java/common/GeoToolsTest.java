package common;

import org.geotools.geometry.jts.JTSFactoryFinder;
import org.locationtech.jts.geom.GeometryFactory;
import org.locationtech.jts.geom.LineString;
import org.locationtech.jts.geom.Point;
import org.locationtech.jts.geom.Polygon;
import org.locationtech.jts.io.ParseException;
import org.locationtech.jts.io.WKTReader;


/***
 * 空间位置和空间关系
 * 相等（Equals）:几何形状拓扑上相等
 * 脱节（Disjoint）:几何形状没有共有的点
 * 相交（Intersects）:几何形状至少有一个共有点（区别脱节）
 * 接触（Touches）:几何形状有至少一个公共的边界点，但是没有内部点
 * 交叉（Crosses）:几何形状共享一些但不是所有的内部点
 * 内含（Within）:几何形状A的线都在几何形状B内部
 * 包含（Contains）:几何形状B的线都在几何形状A内部（区别于内含）
 * 重叠（Overlaps）:几何形状共享一部分但不是所有的公共点，而且相交处有他们自己相同的区域
 */

public class GeoToolsTest {
    public static void main(String[] args) throws ParseException {

        String wktPoint = "POINT(103.83489981581 33.462715497945)";
        String wktLine = "LINESTRING(108.32803893589 41.306670233001,99.950999898452 25.84722546391)";
        String wktPolygon = "POLYGON((100.02715479879 32.168082192159,102.76873121104 37.194305614622,107.0334056301 34.909658604412,105.96723702534 30.949603786713,100.02715479879 32.168082192159))";
        String wktPolygon1 = "POLYGON((96.219409781775 32.777321394882,96.219409781775 40.240501628236,104.82491352023001 40.240501628236,104.82491352023001 32.777321394882,96.219409781775 32.777321394882))";

        //空间关系
        GeometryFactory geometryFactory = JTSFactoryFinder.getGeometryFactory( null );
        WKTReader reader = new WKTReader( geometryFactory );
        Point point = (Point) reader.read(wktPoint);
        LineString line = (LineString) reader.read(wktLine);
        Polygon polygon = (Polygon) reader.read(wktPolygon);
        Polygon polygon1 = (Polygon) reader.read(wktPolygon1);
        System.out.println("-------空间关系判断-------");
        System.out.println("包含："+polygon.contains(point));
        System.out.println("相交："+polygon.intersects(line));
        System.out.println("重叠："+polygon.overlaps(polygon1));
    }
}
