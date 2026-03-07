package dataset4j;

import java.util.Objects;
import java.util.function.BiFunction;
import java.util.function.Function;

/**
 * A generic pair of values, used as the default join result type.
 *
 * <p>For inner joins both {@code left} and {@code right} are non-null.
 * For left joins, {@code right} may be null when there is no match.
 *
 * <pre>
 * // Inner join — no custom record needed
 * Dataset&lt;Pair&lt;Employee, Department&gt;&gt; joined =
 *     employees.innerJoin(departments, Employee::dept, Department::dept);
 *
 * // Access fields naturally
 * joined.filter(p -> p.left().age() > 25)
 *       .map(p -> p.left().name() + " works in " + p.right().location());
 *
 * // Convert to a custom record when you need to
 * joined.map(p -> new EmpLocation(p.left().name(), p.right().location()));
 * </pre>
 *
 * @param left  the left (driving) row
 * @param right the right (looked-up) row — may be null in left joins
 */
public record Pair<L, R>(L left, R right) {

    /** Map both sides into a new value. */
    public <V> V map(BiFunction<L, R, V> mapper) {
        return mapper.apply(left, right);
    }

    /** Transform the left side, keeping right unchanged. */
    public <L2> Pair<L2, R> mapLeft(Function<L, L2> mapper) {
        return new Pair<>(mapper.apply(left), right);
    }

    /** Transform the right side, keeping left unchanged. */
    public <R2> Pair<L, R2> mapRight(Function<R, R2> mapper) {
        return new Pair<>(left, mapper.apply(right));
    }

    /** True if the right side is present (matched in a left join). */
    public boolean hasRight() {
        return right != null;
    }

    /** True if the left side is present (matched in a right join). */
    public boolean hasLeft() {
        return left != null;
    }


    /** Destructure with positional accessors (alias for left/right). */
    public L first()  { return left; }
    public R second() { return right; }

    @Override
    public String toString() {
        return "(" + left + ", " + right + ")";
    }
}
